package com.mzinx.mongodb.changestream.service;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.bson.BsonString;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mzinx.mongodb.changestream.ChangeStreamRegistry;
import com.mzinx.mongodb.changestream.config.ChangeStreamProperties;
import com.mzinx.mongodb.changestream.model.ChangeStream;
import com.mzinx.mongodb.changestream.model.ChangeStream.Mode;
import com.mzinx.mongodb.changestream.model.ChangeStream.ResumeStrategy;
import com.mzinx.mongodb.changestream.model.ChangeStreamCoordination;
import com.mzinx.mongodb.changestream.model.ChangeStreamRuntime;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Runs change streams on this instance and converges their local state to the
 * distributed desired state stored in the coordination collection.
 * <p>
 * Design principles:
 * <ul>
 * <li><b>The coordination document is the single source of truth.</b> All
 * distributed decisions (who runs, who leads, how the stream is partitioned)
 * are derived from the document returned by the atomic updates in
 * {@link ChangeStreamCoordinator}; the in-memory runtime only caches it.</li>
 * <li><b>Reconciliation, not choreography.</b> {@link #reconcile} is an
 * idempotent desired-state convergence step. It is executed periodically by
 * the manager (authoritative) and eagerly on coordination events
 * (latency optimization). Losing an event is therefore harmless.</li>
 * <li><b>Per-stream serialization.</b> All state transitions of one change
 * stream go through a per-stream lock, eliminating races between the
 * scheduler, coordination events and instance events.</li>
 * <li><b>Fencing.</b> AUTO_RECOVER leadership is a server-time lease with a
 * monotonic term; resume token checkpoints are stamped with the term and
 * resume selection prefers the highest term, so a deposed leader's stale
 * checkpoints can never move the legitimate resume point.</li>
 * </ul>
 * Modes:
 * <ul>
 * <li>{@link Mode#BROADCAST} — every registered member runs the full stream;
 * no leader is needed or elected.</li>
 * <li>{@link Mode#AUTO_RECOVER} — exactly one instance runs the stream: the
 * holder of the leader lease. Failover happens when the lease expires or the
 * holder's heartbeat dies.</li>
 * <li>{@link Mode#AUTO_SCALE} — every member runs a disjoint hash partition
 * of the stream. Partitions are derived from the <b>sorted member list and
 * membership epoch of the coordination document</b>, so all instances compute
 * identical, non-overlapping partitions and repartition exactly once per
 * membership change.</li>
 * </ul>
 */
@Service
public class ChangeStreamService<T> {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private static final String INDEX_NAME = "ttl";
	private static final String CSID_FIELD = "_id.cs";
	private static final String HOST_FIELD = "_id.h";
	private static final String DATE_FIELD = "at";
	private static final String TOKEN_FIELD = "t";
	private static final String TERM_FIELD = "term";

	private static final int CHECKPOINT_MAX_RETRIES = 3;
	private static final long CHECKPOINT_RETRY_DELAY = 500; // ms

	@Autowired
	private MongoTemplate mongoTemplate;
	/** Shared executor for event fan-out and reconcile requests. */
	@Autowired
	private Executor taskExecutor;

	@Autowired
	private ChangeStreamRegistry changeStreamRegistry;

	@Autowired
	private ChangeStreamCoordinator coordinator;

	@Autowired
	private ChangeStreamProperties changeStreamProperties;

	/**
	 * Dedicated executor for the blocking watch loops, so long-running cursors
	 * can never starve the application task executor.
	 */
	private ExecutorService watchExecutor;

	private final Set<Consumer<ChangeStreamDocument<?>>> listeners = new CopyOnWriteArraySet<>();

	/** Per-stream locks serializing every state transition of a stream. */
	private final ConcurrentMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

	@PostConstruct
	private void init() {
		this.watchExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
			private final AtomicLong counter = new AtomicLong();

			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "change-stream-watch-" + counter.incrementAndGet());
				t.setDaemon(true);
				return t;
			}
		});

		MongoCollection<Document> tokens = mongoTemplate
				.getCollection(changeStreamProperties.getResumeTokenCollection());
		try {
			createTokenIndex(tokens);
		} catch (MongoCommandException e) {
			// index exists with different options (e.g. changed tokenMaxLifeTime)
			if (e.getErrorCode() == 85 || e.getErrorCode() == 86) {
				tokens.dropIndex(INDEX_NAME);
				createTokenIndex(tokens);
			} else {
				throw e;
			}
		}

		// fast liveness path: when an instance document is deleted (active sweep
		// or TTL), immediately repair memberships/leases and re-converge. The
		// periodic reconcile loop provides the same repair authoritatively, so
		// losing this event is harmless.
		this.subscribe(event -> {
			if (event.getNamespace() == null || !event.getNamespace().getCollectionName()
					.equals(changeStreamProperties.getInstanceCollection()))
				return;
			try {
				switch (event.getOperationType()) {
					case DELETE:
						List<String> alive = coordinator.aliveInstances();
						if (!alive.isEmpty())
							coordinator.repair(alive);
						this.requestReconcileAll();
						break;
					default:
						break;
				}
			} catch (RuntimeException e) {
				logger.error("Unexpected error while processing instance changes:", e);
			}
		});
	}

	private void createTokenIndex(MongoCollection<Document> tokens) {
		tokens.createIndex(Indexes.descending(DATE_FIELD),
				new IndexOptions()
						.expireAfter(changeStreamProperties.getTokenMaxLifeTime(), TimeUnit.MILLISECONDS)
						.name(INDEX_NAME));
	}

	@PreDestroy
	private void destroy() {
		for (String streamId : changeStreamRegistry.ids()) {
			ChangeStreamRuntime<T> runtime = changeStreamRegistry.deregister(streamId);
			if (runtime == null)
				continue;
			try {
				this.stop(runtime);
			} catch (RuntimeException e) {
				logger.error("Error stopping change stream " + streamId + " on shutdown:", e);
			}
		}
		this.clear();
		if (this.watchExecutor != null)
			this.watchExecutor.shutdownNow();
	}

	/**
	 * Registers the change stream on this instance and converges it to the
	 * distributed desired state (joining the member list, acquiring the leader
	 * lease when applicable and starting the local watch loop if this host
	 * should run it).
	 */
	public void start(ChangeStreamRuntime<T> runtime) {
		this.logger.info("Start change stream: " + runtime.getChangeStream().getId());
		this.changeStreamRegistry.register(runtime.getChangeStream().getId(), runtime);
		this.reconcile(runtime);
	}

	/**
	 * Stops the local watch loop and deregisters this host from the
	 * coordination document; other instances keep running the stream.
	 */
	public void stop(ChangeStreamRuntime<T> runtime) {
		this.doStop(runtime, false);
	}

	/**
	 * Stops the local watch loop and clears the whole coordination state, so
	 * every instance running this change stream stops on its next reconcile.
	 */
	public void stopAllInstances(ChangeStreamRuntime<T> runtime) {
		this.doStop(runtime, true);
	}

	private void doStop(ChangeStreamRuntime<T> runtime, boolean stopAll) {
		String streamId = runtime.getChangeStream().getId();
		this.logger.info("Stop change stream: " + streamId);
		ReentrantLock lock = lockFor(streamId);
		lock.lock();
		try {
			this.stopLocal(runtime);
			if (stopAll)
				coordinator.reset(streamId);
			else
				coordinator.leave(streamId);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Converges the local state of the change stream to the coordination
	 * document (single source of truth): synchronizes the runtime cache from
	 * the database, then starts, stops or repartitions the local watch loop as
	 * required by the mode. Idempotent; safe to call from the periodic loop
	 * and from event triggers concurrently (transitions are serialized by a
	 * per-stream lock). Errors are logged and healed by the next cycle.
	 */
	public void reconcile(ChangeStreamRuntime<T> runtime) {
		String streamId = runtime.getChangeStream().getId();
		ReentrantLock lock = lockFor(streamId);
		lock.lock();
		try {
			// skip registries that were deregistered while this task was queued
			if (this.changeStreamRegistry.<T>get(streamId) != runtime)
				return;
			this.doReconcile(runtime);
		} catch (RuntimeException e) {
			logger.error("Unable to reconcile change stream " + streamId + " (will retry on next cycle):", e);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Asynchronously reconciles the registered change stream, used by event
	 * listeners as a low-latency nudge without blocking the event loop.
	 */
	public void requestReconcile(String streamId) {
		ChangeStreamRuntime<T> runtime = this.changeStreamRegistry.get(streamId);
		if (runtime == null)
			return;
		try {
			CompletableFuture.runAsync(() -> this.reconcile(runtime), taskExecutor);
		} catch (RuntimeException e) {
			logger.debug("Unable to schedule reconcile for " + streamId, e);
		}
	}

	/** Asynchronously reconciles every registered change stream. */
	public void requestReconcileAll() {
		this.changeStreamRegistry.ids().forEach(this::requestReconcile);
	}

	private void doReconcile(ChangeStreamRuntime<T> runtime) {
		ChangeStream<T> stream = runtime.getChangeStream();
		String streamId = stream.getId();
		String hostname = changeStreamProperties.getHostname();

		// membership registration doubles as the DB -> memory synchronization
		// read: the returned document is the authoritative state
		ChangeStreamCoordination coordination = coordinator.join(streamId);
		if (Mode.AUTO_RECOVER == stream.getMode())
			coordination = coordinator.acquireOrRenewLease(streamId);
		this.apply(runtime, coordination);

		switch (stream.getMode()) {
			case BROADCAST:
				// every member runs the full stream, no leader involved
				if (coordination.isMember(hostname) && !runtime.isActive()) {
					this.launch(runtime);
				} else if (!coordination.isMember(hostname) && runtime.isActive()) {
					this.stopLocal(runtime);
				}
				break;
			case AUTO_RECOVER:
				// only the lease holder runs; everyone else stands by
				if (coordination.isLeader(hostname)) {
					if (!runtime.isActive())
						this.launch(runtime);
				} else if (runtime.isActive()) {
					this.logger.info("Not the leader of " + streamId + " anymore, stopping local runner");
					this.stopLocal(runtime);
				}
				break;
			case AUTO_SCALE:
				// deterministic partitioning: every instance derives its
				// partition from the same sorted member list, guarded by the
				// membership epoch so all instances repartition exactly once
				// per membership change
				int index = coordination.getMembers().indexOf(hostname);
				int size = coordination.getMembers().size();
				if (index >= 0) {
					if (!runtime.isActive()) {
						this.partition(runtime, index, size, coordination.getEpoch());
						this.launch(runtime);
					} else if (runtime.getAppliedEpoch() != coordination.getEpoch()) {
						this.logger.info("Membership epoch of " + streamId + " changed ("
								+ runtime.getAppliedEpoch() + " -> " + coordination.getEpoch()
								+ "), repartitioning " + (index + 1) + "/" + size);
						this.stopLocal(runtime);
						this.partition(runtime, index, size, coordination.getEpoch());
						this.launch(runtime);
					}
				} else if (runtime.isActive()) {
					this.stopLocal(runtime);
				}
				break;
			default:
				break;
		}
	}

	/** Synchronizes the runtime cache from the coordination document. */
	private void apply(ChangeStreamRuntime<T> runtime, ChangeStreamCoordination coordination) {
		runtime.setLeader(coordination.getLeader());
		runtime.setLeaseUntil(coordination.getLeaseUntil());
		runtime.setTerm(coordination.getTerm());
		runtime.setInstances(coordination.getMembers());
		runtime.setEpoch(coordination.getEpoch());
	}

	private void partition(ChangeStreamRuntime<T> runtime, int index, int size, long epoch) {
		runtime.setPartitionIndex(index);
		runtime.setPartitionCount(size);
		runtime.setAppliedEpoch(epoch);
	}

	private void launch(ChangeStreamRuntime<T> runtime) {
		this.prepareResumeToken(runtime);
		this.run(runtime);
	}

	/**
	 * Selects the resume token to start from, according to the mode:
	 * <ul>
	 * <li>BROADCAST — this host's own checkpoint (each member has its own
	 * position); falls back to the oldest checkpoint of any host for new
	 * members.</li>
	 * <li>AUTO_RECOVER — the checkpoint of the highest fencing term (the last
	 * legitimate leader), latest first; checkpoints of deposed leaders carry
	 * older terms and are ignored.</li>
	 * <li>AUTO_SCALE — the oldest checkpoint across all hosts, so no partition
	 * loses events over a repartition (at-least-once).</li>
	 * </ul>
	 */
	private void prepareResumeToken(ChangeStreamRuntime<T> runtime) {
		ChangeStream<T> stream = runtime.getChangeStream();
		if (ResumeStrategy.NONE == stream.getResumeStrategy())
			return;
		MongoCollection<Document> tokens = mongoTemplate
				.getCollection(changeStreamProperties.getResumeTokenCollection());
		String streamId = stream.getId();
		Document checkpoint = null;
		switch (stream.getMode()) {
			case AUTO_RECOVER:
				checkpoint = tokens.find(Filters.eq(CSID_FIELD, streamId))
						.sort(Sorts.descending(TERM_FIELD, DATE_FIELD)).limit(1).first();
				break;
			case BROADCAST:
				checkpoint = tokens.find(Filters.and(
						Filters.eq(CSID_FIELD, streamId),
						Filters.eq(HOST_FIELD, changeStreamProperties.getHostname()))).first();
				if (checkpoint == null)
					checkpoint = tokens.find(Filters.eq(CSID_FIELD, streamId))
							.sort(Sorts.ascending(DATE_FIELD)).limit(1).first();
				break;
			case AUTO_SCALE:
			default:
				checkpoint = tokens.find(Filters.eq(CSID_FIELD, streamId))
						.sort(Sorts.ascending(DATE_FIELD)).limit(1).first();
				break;
		}
		stream.setResumeToken(checkpoint == null ? null : checkpoint.getString(TOKEN_FIELD));
		if (checkpoint != null)
			this.logger.info("Resume change stream " + streamId + " from checkpoint of "
					+ checkpoint.get("_id") + " at " + checkpoint.getDate(DATE_FIELD));
	}

	/**
	 * Claims the run flag and schedules the blocking watch loop on the
	 * dedicated watch executor. Errors never restart the stream from inside
	 * the watch task (which previously could deadlock joining its own future);
	 * they stop the loop and request an asynchronous reconcile that restarts
	 * the stream if this host should still run it.
	 */
	private void run(ChangeStreamRuntime<T> runtime) {
		ChangeStream<T> stream = runtime.getChangeStream();
		String streamId = stream.getId();
		if (!stream.claim()) {
			this.logger.debug("Change stream " + streamId + " already claimed, skip launch");
			return;
		}
		long term = runtime.getTerm();
		CompletableFuture<Object> completableFuture;
		try {
			completableFuture = this.submitWatch(runtime, term);
		} catch (RuntimeException e) {
			// executor rejected the task (e.g. shutdown): release the claim
			stream.setRunning(false);
			throw e;
		}
		runtime.setCompletableFuture(completableFuture);
		this.logger.info("Change stream " + streamId + " started (term " + term + ")");
	}

	private CompletableFuture<Object> submitWatch(ChangeStreamRuntime<T> runtime, long term) {
		ChangeStream<T> stream = runtime.getChangeStream();
		String streamId = stream.getId();
		return CompletableFuture.supplyAsync(() -> {
			try {
				if (runtime.getCollectionName() != null) {
					stream.watch(mongoTemplate.getCollection(runtime.getCollectionName()), runtime,
							resumeToken -> saveCheckpoint(streamId, resumeToken, term));
				} else {
					stream.watch(mongoTemplate.getDb(), runtime,
							resumeToken -> saveCheckpoint(streamId, resumeToken, term));
				}
			} catch (MongoInterruptedException e) {
				this.logger.info("Change stream '" + streamId + "' interrupted");
			} catch (RuntimeException e) {
				if (isNonResumable(e) && stream.getResumeToken() != null) {
					this.logger.warn("Change stream '" + streamId + "' cannot resume from token '"
							+ stream.getResumeToken()
							+ "' (out of oplog window, a token of an incompatible pipeline"
							+ " e.g. after AUTO_SCALE repartitioning, or an unusable token"
							+ " e.g. checkpointed while the watched collection/database was"
							+ " dropped), discarding the checkpoint and restarting: " + e.getMessage());
					this.discardCheckpoints(streamId, stream.getResumeToken());
					stream.setResumeToken(null);
				} else {
					this.logger.error("Change stream '" + streamId + "' stopped due to unexpected error:", e);
				}
				stream.setRunning(false);
				// fire-and-forget: the reconcile restarts the stream if this
				// host should still run it, without joining our own future
				this.requestReconcile(streamId);
			}
			return null;
		}, watchExecutor);
	}

	/**
	 * Whether the error means the stream can never be resumed from the current
	 * resume token: the token fell out of the oplog window
	 * ({@code ChangeStreamHistoryLost}), it is not part of the stream's token
	 * series ({@code ChangeStreamFatalError}, e.g. checkpoints written by a
	 * differently partitioned AUTO_SCALE pipeline), or the server rejects the
	 * token outright ({@code InvalidResumeToken}, e.g. a checkpoint taken from
	 * an invalidate notification when the watched collection or database was
	 * dropped - note this error carries no {@code NonResumableChangeStreamError}
	 * label). Recovery is to discard the checkpoint and restart from now
	 * (at-most-once for the lost window).
	 */
	private static boolean isNonResumable(RuntimeException e) {
		if (!(e instanceof MongoException mongoException))
			return false;
		return mongoException.hasErrorLabel("NonResumableChangeStreamError")
				|| mongoException.getCode() == 260 // InvalidResumeToken
				|| mongoException.getCode() == 280 // ChangeStreamFatalError
				|| mongoException.getCode() == 286; // ChangeStreamHistoryLost
	}

	/**
	 * Deletes every checkpoint of the stream carrying the poisoned token,
	 * regardless of the host that wrote it: resume selection may pick another
	 * host's checkpoint (AUTO_RECOVER highest term, AUTO_SCALE/BROADCAST
	 * fallback oldest), so deleting only our own document could resume from
	 * the same invalid token forever. If other stale checkpoints remain, each
	 * restart discards one more until the stream converges.
	 */
	private void discardCheckpoints(String streamId, String token) {
		try {
			mongoTemplate.getCollection(changeStreamProperties.getResumeTokenCollection()).deleteMany(
					Filters.and(Filters.eq(CSID_FIELD, streamId), Filters.eq(TOKEN_FIELD, token)));
		} catch (RuntimeException cleanup) {
			this.logger.warn("Unable to delete invalid checkpoint of " + streamId, cleanup);
		}
	}

	/** Stops the local watch loop without touching the coordination state. */
	private void stopLocal(ChangeStreamRuntime<T> runtime) {
		try {
			runtime.stop();
		} catch (CompletionException e) {
			this.logger.debug("Watch loop of " + runtime.getChangeStream().getId() + " ended exceptionally", e);
		}
	}

	/**
	 * Persists the resume token checkpoint of this host, stamped with the
	 * leader term of the run that produced it. Checkpoints are per-host
	 * documents, so a deposed (zombie) leader only ever writes its own
	 * document with its stale term; resume selection prefers the highest term
	 * (see {@link #prepareResumeToken}), so stale checkpoints can never move
	 * the legitimate resume point (soft fencing without lockout risk).
	 */
	private void saveCheckpoint(String streamId, BsonString token, long term) {
		logger.debug("Save checkpoint of " + streamId + " (term " + term + ")");
		MongoException last = null;
		for (int attempt = 1; attempt <= CHECKPOINT_MAX_RETRIES; attempt++) {
			try {
				mongoTemplate.getCollection(changeStreamProperties.getResumeTokenCollection()).updateOne(
						Filters.and(
								Filters.eq(CSID_FIELD, streamId),
								Filters.eq(HOST_FIELD, changeStreamProperties.getHostname())),
						Updates.combine(
								Updates.set(DATE_FIELD, new Date()),
								Updates.set(TOKEN_FIELD, token),
								Updates.set(TERM_FIELD, term)),
						new UpdateOptions().upsert(true));
				return;
			} catch (MongoException e) {
				last = e;
			}
			try {
				Thread.sleep(CHECKPOINT_RETRY_DELAY * attempt);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		throw last;
	}

	public void publish(ChangeStreamDocument<T> event) {
		logger.debug("new event:" + event);
		listeners.forEach(l -> {
			CompletableFuture.supplyAsync(() -> {
				try {
					l.accept(event);
				} catch (RuntimeException e) {
					logger.error("Unexpected error publishing event:", e);
				}
				return null;
			}, taskExecutor);
		});
	}

	public void subscribe(Consumer<ChangeStreamDocument<?>> listener) {
		logger.info("new subscription:" + listeners.add(listener));
	}

	public void unsubscribe(Consumer<ChangeStreamDocument<?>> listener) {
		logger.info("remove subscription:" + listeners.remove(listener));
	}

	public void clear() {
		logger.info("Clear all subscription");
		listeners.clear();
	}

	private ReentrantLock lockFor(String streamId) {
		return locks.computeIfAbsent(streamId, k -> new ReentrantLock());
	}
}
