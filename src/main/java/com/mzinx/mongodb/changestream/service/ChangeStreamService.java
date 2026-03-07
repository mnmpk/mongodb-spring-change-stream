package com.mzinx.mongodb.changestream.service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.resilience.annotation.Retryable;
import org.springframework.stereotype.Service;

import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.mongodb.client.result.UpdateResult;
import com.mzinx.mongodb.aggregation.service.AggregationService;
import com.mzinx.mongodb.changestream.config.ChangeStreamProperties;
import com.mzinx.mongodb.changestream.model.ChangeStream;
import com.mzinx.mongodb.changestream.model.ChangeStreamRegistry;
import com.mzinx.mongodb.changestream.model.ChangeStream.Mode;
import com.mzinx.mongodb.changestream.model.ChangeStream.ResumeStrategy;
import com.mzinx.mongodb.aggregation.dao.PipelineRepository;
import com.mzinx.mongodb.aggregation.model.Aggregation;
import com.mzinx.mongodb.aggregation.model.PipelineTemplate;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class ChangeStreamService<T> {
	Logger logger = LoggerFactory.getLogger(getClass());
	private static final String INDEX_NAME = "ttl";
	private static final String CSID_FIELD = "_id.cs";
	private static final String HOST_FIELD = "_id.h";
	private static final String DATE_FIELD = "at";
	private static final String TOKEN_FIELD = "t";
	private static final String CHANGE_STREAMS_FIELD = "cs";

	@Autowired
	private MongoClient mongoClient;
	@Autowired
	private MongoTemplate mongoTemplate;
	@Autowired
	private Executor taskExecutor;

	@Autowired
	private Map<String, ChangeStreamRegistry<T>> changeStreams;

	@Autowired
	private AggregationService aggregationService;
	@Autowired
	private PipelineRepository pipelineRepository;

	private final String podName = System.getenv("HOSTNAME");

	@Autowired
	private Set<String> instances;

	@Autowired
	private ChangeStreamProperties changeStreamProperties;

	private static final Set<Consumer<ChangeStreamDocument<?>>> listeners = new HashSet<>();

	@PostConstruct
	private void init() {
		mongoTemplate.getCollection(changeStreamProperties.getResumeTokenCollection())
				.createIndex(Indexes.descending(DATE_FIELD),
						new IndexOptions()
								.expireAfter(changeStreamProperties.getTokenMaxLifeTime(), TimeUnit.MILLISECONDS)
								.name(INDEX_NAME));
		this.subscribe(event -> {
			if (event.getNamespace().getCollectionName().equals(changeStreamProperties.getInstanceCollection())) {
				try {
					if (OperationType.INSERT == event.getOperationType()
							|| OperationType.DELETE == event.getOperationType()) {
						logger.info("change streams running in this node " + changeStreams.keySet());
					}
					String instance = event.getDocumentKey().getString("_id").getValue();
					for (String csId : changeStreams.keySet()) {
						ChangeStreamRegistry<T> reg = changeStreams.get(csId);
						ChangeStream<T> cs = reg.getChangeStream();
						switch (event.getOperationType()) {
							case INSERT:
								if (Mode.AUTO_SCALE == cs.getMode()) {
									this._stop(reg);
									run(reg, event.getFullDocument() != null
											? ((Document) event.getFullDocument()).getDate("at")
											: null);
								} else {
									reg.getInstances().add(instance);
								}
								break;
							case UPDATE:
								if (Mode.AUTO_RECOVER == cs.getMode()) {
									UpdateDescription ud = event.getUpdateDescription();
									if (ud != null) {
										if (ud.getRemovedFields().contains(CHANGE_STREAMS_FIELD)) {
											logger.info(
													"stopping change stream " + csId);
											this._stop(reg);
										} else {
											BsonDocument updatedFields = ud.getUpdatedFields();
											if (updatedFields != null
													&& updatedFields.containsKey(CHANGE_STREAMS_FIELD)) {
												if (updatedFields.getArray(CHANGE_STREAMS_FIELD)
														.contains(new BsonString(csId))) {
													if (!reg.getChangeStream().isRunning()) {
														// restart change stream
														this.run(reg);
													}
												} else {
													logger.info(
															"stopping change stream " + csId);
													this._stop(reg);
												}
											}
										}
									}
								} else if (Mode.AUTO_SCALE == cs.getMode()) {
									if (!cs.isRunning()) {
										logger.info(
												cs.getId() + " is not yet running in " + podName + " start it now.");
										scale(reg);
									}
								}
								break;
							case DELETE:
								if (Mode.AUTO_RECOVER == cs.getMode()) {
									if (podName.equals(instance)) {
										if (changeStreams.containsKey(cs.getId())
												&& cs.isRunning()) {
											logger.info(
													"Change stream running in this node should stop.");
											this._stop(reg);
										}
									} else {
										if (changeStreams.containsKey(cs.getId())
												&& changeStreams.get(cs.getId()).getChangeStream().isRunning()) {
											logger.info(
													instance + " is dead, I'm still running change stream:"
															+ cs.getId());
										} else {
											logger.info(
													instance + " is dead, " + podName
															+ " try to take over change stream:"
															+ cs.getId());
											this._stop(reg);
											run(reg);
										}
									}
								} else if (Mode.AUTO_SCALE == cs.getMode()) {
									this._stop(reg);
									run(reg, ((Document) event.getFullDocumentBeforeChange()).getDate("at"));
								} else {
									reg.getInstances().remove(instance);
								}
								break;
							default:
								break;
						}
					}
				} catch (RuntimeException e) {
					logger.error("Unexpected error while processing node changes:", e);
				}
			}
		});
	}

	@PreDestroy
	private void destroy() {
		this.clear();
		for (String csId : changeStreams.keySet()) {
			changeStreams.get(csId).getChangeStream().setRunning(false);
			changeStreams.remove(csId);
		}
	}

	public void run(ChangeStreamRegistry<T> reg) {
		this.run(reg, null);
	}

	@Retryable(includes = { MongoException.class }, maxRetries = 10, delay = 5000, multiplier = 2)
	public void run(ChangeStreamRegistry<T> reg, Date earliest) {
		logger.info("Start running change stream");
		changeStreams.put(reg.getChangeStream().getId(), reg);
		ChangeStream<T> cs = reg.getChangeStream();

		logger.info("ResumeStrategy:" + cs.getResumeStrategy());
		if (ResumeStrategy.NONE != cs.getResumeStrategy()) {
			PipelineTemplate p = this.pipelineRepository.findByName("csScaleToken");
			if (p == null || p.getContent() == null)
				throw new RuntimeException("Pipeline not found");
			Aggregation<Document> agg = Aggregation.of(changeStreamProperties.getResumeTokenCollection(),
					p.getContent());
			Map<String, Object> variables = new HashMap<>();
			variables.put("earliest", earliest);
			List<Document> l = aggregationService.execute(agg, variables);
			if (l.size() > 0) {
				cs.resumeAfter(l.get(0).getString(TOKEN_FIELD));
			}
		}

		logger.info("Mode:" + cs.getMode());
		if (Mode.AUTO_RECOVER == cs.getMode()) {
			final ClientSession clientSession = mongoClient.startSession();
			try {
				clientSession.withTransaction(new TransactionBody<Void>() {
					@Override
					public Void execute() {
						Document doc = mongoTemplate.getCollection(changeStreamProperties.getInstanceCollection())
								.find(clientSession, Filters.eq(CHANGE_STREAMS_FIELD, cs.getId())).first();
						if (doc == null) {
							UpdateResult ur = mongoTemplate
									.getCollection(changeStreamProperties.getInstanceCollection()).updateOne(
											clientSession,
											Filters.eq("_id", podName),
											Updates.combine(Updates.set("_id", podName),
													Updates.set(DATE_FIELD,
															Instant.now().plus(changeStreamProperties.getMaxTimeout(),
																	ChronoUnit.MILLIS)),
													Updates.addToSet(CHANGE_STREAMS_FIELD, cs.getId())),
											new UpdateOptions().upsert(true));
							logger.info("Update result:" + ur);
							if (ur.getUpsertedId() != null || ur.getModifiedCount() > 0) {
								logger.info(podName + " wins");
								start(reg);
							}
						} else {
							reg.getInstances().add(doc.getString("_id"));
							if (podName.equals(doc.getString("_id"))) {
								if (changeStreams.containsKey(cs.getId())
										&& cs.isRunning()) {
									logger.info("Change stream already running");
								} else {
									start(reg);
								}
							} else {
								logger.info("Change stream already running in other node:" + doc);
							}
						}
						return null;
					}

				}, TransactionOptions.builder()
						.writeConcern(WriteConcern.MAJORITY).readConcern(ReadConcern.MAJORITY)
						.readPreference(ReadPreference.primary())
						.build());
			} catch (RuntimeException e) {
				logger.error("Unexpected error while registering change stream " + cs.getId() + ":", e);
				throw e;
			} finally {
				clientSession.close();
			}
		} else if (Mode.AUTO_SCALE == cs.getMode()) {
			scale(reg);
		} else {
			start(reg);
		}
	}

	private void start(ChangeStreamRegistry<T> reg) {
		CompletableFuture<Object> completableFuture = CompletableFuture.supplyAsync(() -> {
			try {
				if (reg.getCollectionName() != null) {
					reg.getChangeStream().watch(mongoTemplate.getCollection(reg.getCollectionName()), reg,
							resumeToken -> {
								saveCheckpoint(reg.getChangeStream().getId(), resumeToken);
							});
				} else {
					reg.getChangeStream().watch(mongoTemplate.getDb(), reg, resumeToken -> {
						saveCheckpoint(reg.getChangeStream().getId(), resumeToken);
					});
				}
			} catch (RuntimeException e) {
				logger.error("Stopping change stream '" + reg.getChangeStream().getId() + "'' due to unexpected error:",
						e);
				this._stop(reg);
				// Recover for unexpected exception
				this.run(reg);
			}
			return null;
		}, taskExecutor);
		reg.setCompletableFuture(completableFuture);
		reg.getInstances().add(podName);
		logger.info(reg.getInstances() + " are running change stream " + reg.getChangeStream().getId());
		logger.info("Change stream " + reg.getChangeStream().getId() + " started");
	}

	public void stop(ChangeStreamRegistry<T> reg) {
		logger.info("Stop running change stream");
		this._stop(reg);
		mongoTemplate.getCollection(changeStreamProperties.getInstanceCollection()).findOneAndUpdate(
				Filters.eq(CHANGE_STREAMS_FIELD, reg.getChangeStream().getId()),
				Updates.pull(CHANGE_STREAMS_FIELD, podName));
	}

	public void _stop(ChangeStreamRegistry<T> reg) {
		reg.getChangeStream().setRunning(false);
		reg.getInstances().remove(podName);
		if (reg.getCompletableFuture() != null)
			reg.getCompletableFuture().join();
	}

	private void scale(ChangeStreamRegistry<T> reg) {
		ChangeStream<T> cs = reg.getChangeStream();
		int index = new ArrayList<String>(this.instances).indexOf(podName);
		reg.setInstanceSize(this.instances.size());
		reg.setInstanceIndex(index);
		reg.getInstances().clear();
		reg.getInstances().addAll(this.instances);
		if (this.instances.size() > 0 && index >= 0) {
			if (cs.isRunning()) {
				this._stop(reg);
			}
			logger.info("change stream " + (index + 1) + " of " + this.instances.size()
					+ " start running in "
					+ podName);
			start(reg);
		} else {
			logger.info("Node " + podName + " is not ready to run change stream:" + cs.getId());
		}
	}

	@Retryable(includes = { MongoException.class }, maxRetries = 10, delay = 1000, multiplier = 2)
	private void saveCheckpoint(String csId, BsonString token) {
		logger.info("save checkpoint");
		mongoTemplate.getCollection(changeStreamProperties.getResumeTokenCollection()).updateOne(
				Filters.and(Filters.eq(CSID_FIELD, csId), Filters.eq(HOST_FIELD, podName)),
				Updates.combine(Updates.set(CSID_FIELD, csId),
						Updates.combine(Updates.set(HOST_FIELD, podName),
								Updates.set(DATE_FIELD, new Date()),
								Updates.set(TOKEN_FIELD, token))),
				new UpdateOptions().upsert(true));
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

	public void unsubscribe(Consumer<ChangeStreamDocument<T>> listener) {
		logger.info("remove subscription:" + listeners.remove(listener));
	}

	public void clear() {
		logger.info("Clear all subscription");
		listeners.clear();
	}
}
