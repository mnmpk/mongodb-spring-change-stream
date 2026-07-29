package com.mzinx.mongodb.changestream.service;

import java.util.Date;
import java.util.List;
import java.util.Map;
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
import com.mzinx.mongodb.changestream.config.ChangeStreamProperties;
import com.mzinx.mongodb.changestream.model.ChangeStream;
import com.mzinx.mongodb.changestream.model.ChangeStream.Mode;
import com.mzinx.mongodb.changestream.model.ChangeStream.ResumeStrategy;
import com.mzinx.mongodb.changestream.model.ChangeStreamCoordination;
import com.mzinx.mongodb.changestream.model.ChangeStreamRegistry;

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
 * {@link ChangeStreamCoordinator}; the in-memory registry only caches it.</li>
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
 * <li>{@link Mode#BOARDCAST} — every registered member runs the full stream;
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
	Logger logger = LoggerFactory.getLogger(getClass());
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
	private Map<String, ChangeStreamRegistry<T>> changeStreams;

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
		for (String csId : Set.copyOf(changeStreams.keySet())) {
			ChangeStreamRegistry<T> reg = changeStreams.remove(csId);
			if (reg == null)
				continue;
			try {
				this.stop(reg, false);
			} catch (RuntimeException e) {
				logger.error("Error stopping change stream " + csId + " on shutdown:", e);
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
	public void start(ChangeStreamRegistry<T> reg) {
		this.logger.info("Start change stream: " + reg.getChangeStream().getId());
		this.changeStreams.put(reg.getChangeStream().getId(), reg);
		this.reconcile(reg);
	}

	/**
	 * Stops the local watch loop and deregisters this host from the
	 * coordination document ({@code stopAll = false}), or clears the whole
	 * coordination state so every instance stops ({@code stopAll = true}).
	 */
	public void stop(ChangeStreamRegistry<T> reg, boolean stopAll) {
		String csId = reg.getChangeStream().getId();
		this.logger.info("Stop change stream: " + csId);
		ReentrantLock lock = lockFor(csId);
		lock.lock();
		try {
			this.stopLocal(reg);
			if (stopAll)
				coordinator.reset(csId);
			else
				coordinator.leave(csId);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Converges the local state of the change stream to the coordination
	 * document (single source of truth): synchronizes the registry cache from
	 * the database, then starts, stops or repartitions the local watch loop as
	 * required by the mode. Idempotent; safe to call from the periodic loop
	 * and from event triggers concurrently (transitions are serialized by a
	 * per-stream lock). Errors are logged and healed by the next cycle.
	 */
	public void reconcile(ChangeStreamRegistry<T> reg) {
		String csId = reg.getChangeStream().getId();
		ReentrantLock lock = lockFor(csId);
		lock.lock();
		try {
			// skip registries that were deregistered while this task was queued
			if (this.changeStreams.get(csId) != reg)
				return;
			this.doReconcile(reg);
		} catch (RuntimeException e) {
			logger.error("Unable to reconcile change stream " + csId + " (will retry on next cycle):", e);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Asynchronously reconciles the registered change stream, used by event
	 * listeners as a low-latency nudge without blocking the event loop.
	 */
	public void requestReconcile(String csId) {
		ChangeStreamRegistry<T> reg = this.changeStreams.get(csId);
		if (reg == null)
			return;
		try {
			CompletableFuture.runAsync(() -> this.reconcile(reg), taskExecutor);
		} catch (RuntimeException e) {
			logger.debug("Unable to schedule reconcile for " + csId, e);
		}
	}

	/** Asynchronously reconciles every registered change stream. */
	public void requestReconcileAll() {
		this.changeStreams.keySet().forEach(this::requestReconcile);
	}

	private void doReconcile(ChangeStreamRegistry<T> reg) {
		ChangeStream<T> cs = reg.getChangeStream();
		String csId = cs.getId();
		String hostname = changeStreamProperties.getHostname();

		// membership registration doubles as the DB -> memory synchronization
		// read: the returned document is the authoritative state
		ChangeStreamCoordination coordination = coordinator.join(csId);
		if (Mode.AUTO_RECOVER == cs.getMode())
			coordination = coordinator.acquireOrRenewLease(csId);
		this.apply(reg, coordination);

		switch (cs.getMode()) {
			case BOARDCAST:
				// every member runs the full stream, no leader involved
				if (coordination.isMember(hostname) && !reg.isActive()) {
					this.launch(reg);
				} else if (!coordination.isMember(hostname) && reg.isActive()) {
					this.stopLocal(reg);
				}
				break;
			case AUTO_RECOVER:
				// only the lease holder runs; everyone else stands by
				if (coordination.isLeader(hostname)) {
					if (!reg.isActive())
						this.launch(reg);
				} else if (reg.isActive()) {
					this.logger.info("Not the leader of " + csId + " anymore, stopping local runner");
					this.stopLocal(reg);
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
					if (!reg.isActive()) {
						this.partition(reg, index, size, coordination.getEpoch());
						this.launch(reg);
					} else if (reg.getAppliedEpoch() != coordination.getEpoch()) {
						this.logger.info("Membership epoch of " + csId + " changed ("
								+ reg.getAppliedEpoch() + " -> " + coordination.getEpoch()
								+ "), repartitioning " + (index + 1) + "/" + size);
						this.stopLocal(reg);
						this.partition(reg, index, size, coordination.getEpoch());
						this.launch(reg);
					}
				} else if (reg.isActive()) {
					this.stopLocal(reg);
				}
				break;
			default:
				break;
		}
	}

	/** Synchronizes the registry cache from the coordination document. */
	private void apply(ChangeStreamRegistry<T> reg, ChangeStreamCoordination coordination) {
		reg.setLeader(coordination.getLeader());
		reg.setLeaseUntil(coordination.getLeaseUntil());
		reg.setTerm(coordination.getTerm());
		reg.setInstances(coordination.getMembers());
		reg.setEpoch(coordination.getEpoch());
	}

	private void partition(ChangeStreamRegistry<T> reg, int index, int size, long epoch) {
		reg.setInstanceIndex(index);
		reg.setInstanceSize(size);
		reg.setAppliedEpoch(epoch);
	}

	private void launch(ChangeStreamRegistry<T> reg) {
		this.prepareResumeToken(reg);
		this.run(reg);
	}

	/**
	 * Selects the resume token to start from, according to the mode:
	 * <ul>
	 * <li>BOARDCAST — this host's own checkpoint (each member has its own
	 * position); falls back to the oldest checkpoint of any host for new
	 * members.</li>
	 * <li>AUTO_RECOVER — the checkpoint of the highest fencing term (the last
	 * legitimate leader), latest first; checkpoints of deposed leaders carry
	 * older terms and are ignored.</li>
	 * <li>AUTO_SCALE — the oldest checkpoint across all hosts, so no partition
	 * loses events over a repartition (at-least-once).</li>
	 * </ul>
	 */
	private void prepareResumeToken(ChangeStreamRegistry<T> reg) {
		ChangeStream<T> cs = reg.getChangeStream();
		if (ResumeStrategy.NONE == cs.getResumeStrategy())
			return;
		MongoCollection<Document> tokens = mongoTemplate
				.getCollection(changeStreamProperties.getResumeTokenCollection());
		String csId = cs.getId();
		Document checkpoint = null;
		switch (cs.getMode()) {
			case AUTO_RECOVER:
				checkpoint = tokens.find(Filters.eq(CSID_FIELD, csId))
						.sort(Sorts.descending(TERM_FIELD, DATE_FIELD)).limit(1).first();
				break;
			case BOARDCAST:
				checkpoint = tokens.find(Filters.and(
						Filters.eq(CSID_FIELD, csId),
						Filters.eq(HOST_FIELD, changeStreamProperties.getHostname()))).first();
				if (checkpoint == null)
					checkpoint = tokens.find(Filters.eq(CSID_FIELD, csId))
							.sort(Sorts.ascending(DATE_FIELD)).limit(1).first();
				break;
			case AUTO_SCALE:
			default:
				checkpoint = tokens.find(Filters.eq(CSID_FIELD, csId))
						.sort(Sorts.ascending(DATE_FIELD)).limit(1).first();
				break;
		}
		cs.setResumeToken(checkpoint == null ? null : checkpoint.getString(TOKEN_FIELD));
		if (checkpoint != null)
			this.logger.info("Resume change stream " + csId + " from checkpoint of "
					+ checkpoint.get("_id") + " at " + checkpoint.getDate(DATE_FIELD));
	}

	/**
	 * Claims the run flag and schedules the blocking watch loop on the
	 * dedicated watch executor. Errors never restart the stream from inside
	 * the watch task (which previously could deadlock joining its own future);
	 * they stop the loop and request an asynchronous reconcile that restarts
	 * the stream if this host should still run it.
	 */
	private void run(ChangeStreamRegistry<T> reg) {
		ChangeStream<T> cs = reg.getChangeStream();
		String csId = cs.getId();
		if (!cs.claim()) {
			this.logger.debug("Change stream " + csId + " already claimed, skip launch");
			return;
		}
		long term = reg.getTerm();
		CompletableFuture<Object> completableFuture;
		try {
			completableFuture = this.submitWatch(reg, term);
		} catch (RuntimeException e) {
			// executor rejected the task (e.g. shutdown): release the claim
			cs.setRunning(false);
			throw e;
		}
		reg.setCompletableFuture(completableFuture);
		this.logger.info("Change stream " + csId + " started (term " + term + ")");
	}

	private CompletableFuture<Object> submitWatch(ChangeStreamRegistry<T> reg, long term) {
		ChangeStream<T> cs = reg.getChangeStream();
		String csId = cs.getId();
		return CompletableFuture.supplyAsync(() -> {
			try {
				if (reg.getCollectionName() != null) {
					cs.watch(mongoTemplate.getCollection(reg.getCollectionName()), reg,
							resumeToken -> saveCheckpoint(csId, resumeToken, term));
				} else {
					cs.watch(mongoTemplate.getDb(), reg,
							resumeToken -> saveCheckpoint(csId, resumeToken, term));
				}
			} catch (MongoInterruptedException e) {
				this.logger.info("Change stream '" + csId + "' interrupted");
			} catch (RuntimeException e) {
				if (isNonResumable(e) && cs.getResumeToken() != null) {
					this.logger.warn("Change stream '" + csId + "' cannot resume from token '"
							+ cs.getResumeToken()
							+ "' (out of oplog window, or a token of an incompatible pipeline"
							+ " e.g. after AUTO_SCALE repartitioning), discarding the checkpoint"
							+ " and restarting: " + e.getMessage());
					this.discardCheckpoints(csId, cs.getResumeToken());
					cs.setResumeToken(null);
				} else {
					this.logger.error("Change stream '" + csId + "' stopped due to unexpected error:", e);
				}
				cs.setRunning(false);
				// fire-and-forget: the reconcile restarts the stream if this
				// host should still run it, without joining our own future
				this.requestReconcile(csId);
			}
			return null;
		}, watchExecutor);
	}

	/**
	 * Whether the error means the stream can never be resumed from the current
	 * resume token: the token fell out of the oplog window
	 * ({@code ChangeStreamHistoryLost}), or it is not part of the stream's
	 * token series ({@code ChangeStreamFatalError}, e.g. checkpoints written
	 * by a differently partitioned AUTO_SCALE pipeline). Recovery is to
	 * discard the checkpoint and restart from now (at-most-once for the lost
	 * window).
	 */
	private static boolean isNonResumable(RuntimeException e) {
		if (!(e instanceof MongoException mongoException))
			return false;
		return mongoException.hasErrorLabel("NonResumableChangeStreamError")
				|| mongoException.getCode() == 280 // ChangeStreamFatalError
				|| mongoException.getCode() == 286; // ChangeStreamHistoryLost
	}

	/**
	 * Deletes every checkpoint of the stream carrying the poisoned token,
	 * regardless of the host that wrote it: resume selection may pick another
	 * host's checkpoint (AUTO_RECOVER highest term, AUTO_SCALE/BOARDCAST
	 * fallback oldest), so deleting only our own document could resume from
	 * the same invalid token forever. If other stale checkpoints remain, each
	 * restart discards one more until the stream converges.
	 */
	private void discardCheckpoints(String csId, String token) {
		try {
			mongoTemplate.getCollection(changeStreamProperties.getResumeTokenCollection()).deleteMany(
					Filters.and(Filters.eq(CSID_FIELD, csId), Filters.eq(TOKEN_FIELD, token)));
		} catch (RuntimeException cleanup) {
			this.logger.warn("Unable to delete invalid checkpoint of " + csId, cleanup);
		}
	}

	/** Stops the local watch loop without touching the coordination state. */
	private void stopLocal(ChangeStreamRegistry<T> reg) {
		try {
			reg.stop();
		} catch (CompletionException e) {
			this.logger.debug("Watch loop of " + reg.getChangeStream().getId() + " ended exceptionally", e);
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
	private void saveCheckpoint(String csId, BsonString token, long term) {
		logger.debug("Save checkpoint of " + csId + " (term " + term + ")");
		MongoException last = null;
		for (int attempt = 1; attempt <= CHECKPOINT_MAX_RETRIES; attempt++) {
			try {
				mongoTemplate.getCollection(changeStreamProperties.getResumeTokenCollection()).updateOne(
						Filters.and(
								Filters.eq(CSID_FIELD, csId),
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

	private ReentrantLock lockFor(String csId) {
		return locks.computeIfAbsent(csId, k -> new ReentrantLock());
	}
}
