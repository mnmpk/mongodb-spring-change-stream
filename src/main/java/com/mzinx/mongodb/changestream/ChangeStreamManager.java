package com.mzinx.mongodb.changestream;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mzinx.mongodb.changestream.config.ChangeStreamProperties;
import com.mzinx.mongodb.changestream.listener.ChangeStreamListener;
import com.mzinx.mongodb.changestream.model.ChangeStream;
import com.mzinx.mongodb.changestream.model.ChangeStream.Mode;
import com.mzinx.mongodb.changestream.model.ChangeStreamConfig;
import com.mzinx.mongodb.changestream.model.ChangeStreamRuntime;
import com.mzinx.mongodb.changestream.model.ChangeStreamStatus;
import com.mzinx.mongodb.changestream.service.ChangeStreamConfigService;
import com.mzinx.mongodb.changestream.service.ChangeStreamCoordinator;
import com.mzinx.mongodb.changestream.service.ChangeStreamService;

import jakarta.annotation.PreDestroy;

/**
 * Periodic reconciler driving every change stream registered on this instance
 * towards its desired state. Each cycle performs, in order:
 * <ol>
 * <li><b>coordinate</b> — ensures the coordination change stream (watching the
 * coordination collection) is registered, so leader/membership changes are
 * propagated with low latency; the loop itself remains authoritative when
 * events are lost.</li>
 * <li><b>housekeeping</b> — actively sweeps dead instance heartbeats and
 * atomically repairs every coordination document (removes dead members,
 * releases dead or expired leader leases).</li>
 * <li><b>refresh</b> — reconciles persisted {@link ChangeStreamConfig}s:
 * starts new enabled configs, restarts changed definitions, stops disabled or
 * removed ones.</li>
 * <li><b>reconcileAll</b> — synchronizes every local runtime from its
 * coordination document (the single source of truth) and starts/stops/
 * repartitions the local watch loops per mode.</li>
 * <li><b>orphan cleanup</b> — deletes coordination documents that no config
 * and no instance references anymore.</li>
 * </ol>
 * Runtime status of all registries can be queried via
 * {@link #getChangeStreams()}, {@link #getActiveChangeStreams()} and
 * {@link #getChangeStreamStatus(String)}.
 */
@Component
public class ChangeStreamManager {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String COORDINATION_STREAM_ID = "change-stream";

    private final ApplicationContext context;
    private final ChangeStreamProperties changeStreamProperties;
    private final ChangeStreamService<Document> changeStreamService;
    private final ChangeStreamConfigService changeStreamConfigService;
    private final ChangeStreamCoordinator coordinator;

    /**
     * Shared registry of every change stream runtime on this instance, also
     * populated by {@link ChangeStreamService} for streams started
     * programmatically, so the manager oversees every runtime, including the
     * coordination stream. Config-driven runtimes carry the applied config in
     * {@link ChangeStreamRuntime#getConfig()}.
     */
    private final ChangeStreamRegistry changeStreamRegistry;

    private volatile boolean coordinationStreamStarted = false;

    ChangeStreamManager(ApplicationContext context, ChangeStreamProperties changeStreamProperties,
            ChangeStreamService<Document> changeStreamService, ChangeStreamConfigService changeStreamConfigService,
            ChangeStreamCoordinator coordinator, ChangeStreamRegistry changeStreamRegistry) {
        this.context = context;
        this.changeStreamProperties = changeStreamProperties;
        this.changeStreamService = changeStreamService;
        this.changeStreamConfigService = changeStreamConfigService;
        this.coordinator = coordinator;
        this.changeStreamRegistry = changeStreamRegistry;
    }

    @Scheduled(initialDelayString = "${change-stream.config-refresh-initial-delay:10000}", fixedDelayString = "${change-stream.config-refresh-interval:30000}")
    void reconcileCycle() {
        this.coordinate();
        try {
            this.housekeeping();
        } catch (RuntimeException e) {
            logger.error("Unable to run coordination housekeeping:", e);
        }
        Set<String> configIds = null;
        try {
            configIds = this.refresh();
        } catch (RuntimeException e) {
            logger.error("Unable to refresh change stream configs:", e);
        }
        this.reconcileAll();
        if (configIds != null) {
            try {
                this.cleanOrphans(configIds);
            } catch (RuntimeException e) {
                logger.error("Unable to clean orphaned coordination documents:", e);
            }
        }
    }

    /**
     * Starts the coordination change stream watching the change stream
     * coordination collection, so leader/instance changes are propagated to all
     * nodes with low latency. Its runtime is kept in the shared registry like
     * any other change stream and reconciled by the same loop.
     */
    private void coordinate() {
        if (this.coordinationStreamStarted)
            return;
        try {
            ChangeStreamListener<Document> coordinationChangeListener = this
                    .resolveListener("coordinationChangeListener");
            ChangeStreamRuntime<Document> coordinationRuntime = ChangeStreamRuntime.<Document>builder()
                    .collectionName(changeStreamProperties.getCoordinationCollection())
                    .listener(coordinationChangeListener)
                    .changeStream(ChangeStream.of(COORDINATION_STREAM_ID, Mode.BROADCAST,
                            List.of(Aggregates.match(
                                    Filters.in("operationType", List.of("insert", "update", "delete"))))))
                    .build();
            this.changeStreamService.start(coordinationRuntime);
            this.coordinationStreamStarted = true;
        } catch (RuntimeException e) {
            logger.error("Unable to start coordination change stream:", e);
        }
    }

    /**
     * Active liveness maintenance: deletes stale instance heartbeats (instead
     * of waiting for the TTL monitor) and repairs every coordination document
     * against the fresh alive set (removing dead members and releasing dead or
     * expired leases). Both operations are atomic and idempotent, so every
     * instance can run them on every cycle. Skipped entirely when the instance
     * collection is empty (no discovery/heartbeat mechanism active, liveness
     * unknown).
     */
    private void housekeeping() {
        long swept = coordinator.sweepDeadInstances();
        if (swept > 0)
            logger.info("Swept " + swept + " dead instance(s)");
        List<String> alive = coordinator.aliveInstances();
        if (!alive.isEmpty())
            coordinator.repair(alive);
    }

    /**
     * Fetches change stream configs from the config collection and reconciles
     * them with the currently managed change streams. Returns the ids of all
     * known configs (enabled or not) for the orphan cleanup.
     */
    private Set<String> refresh() {
        List<ChangeStreamConfig> configs = this.changeStreamConfigService.findAll();
        Set<String> known = new HashSet<>();

        for (ChangeStreamConfig config : configs) {
            if (COORDINATION_STREAM_ID.equals(config.getId())) {
                logger.warn("Change stream config id '" + COORDINATION_STREAM_ID
                        + "' is reserved for the coordination stream, ignoring");
                continue;
            }
            known.add(config.getId());
            ChangeStreamRuntime<Document> runtime = this.changeStreamRegistry.get(config.getId());
            // config on the runtime is only set after a successful start, so a
            // half-started runtime is retried on the next refresh
            ChangeStreamConfig current = runtime == null ? null : runtime.getConfig();

            if (!config.isEnabled()) {
                if (current != null) {
                    logger.info("Change stream config " + config.getId() + " disabled, stopping");
                    this.stop(config.getId());
                }
                continue;
            }

            if (current == null) {
                this.start(config);
            } else if (!current.isSameDefinition(config)) {
                logger.info("Change stream config " + config.getId() + " changed, restarting");
                this.stop(config.getId());
                this.start(config);
            }
        }

        // stop change streams whose config was removed
        for (String streamId : this.changeStreamRegistry.ids()) {
            ChangeStreamRuntime<Document> runtime = this.changeStreamRegistry.get(streamId);
            if (runtime != null && runtime.getConfig() != null && !known.contains(streamId)) {
                logger.info("Change stream config " + streamId + " removed, stopping");
                this.stop(streamId);
            }
        }
        return known;
    }

    /**
     * Converges every registered change stream to its coordination document:
     * the database-to-memory synchronization plus the mode state machine, all
     * serialized per stream inside {@link ChangeStreamService#reconcile}.
     */
    private void reconcileAll() {
        for (ChangeStreamRuntime<Document> runtime : this.changeStreamRegistry.<Document>all()) {
            try {
                this.changeStreamService.reconcile(runtime);
            } catch (RuntimeException e) {
                logger.error("Unable to reconcile change stream "
                        + runtime.getChangeStream().getId() + ":", e);
            }
        }
    }

    /**
     * Deletes coordination documents that neither belong to a known config,
     * nor to a locally registered stream, nor have any member left (streams
     * registered programmatically on other instances keep their members and
     * are therefore never touched).
     */
    private void cleanOrphans(Set<String> configIds) {
        Set<String> keep = new HashSet<>(configIds);
        keep.add(COORDINATION_STREAM_ID);
        keep.addAll(this.changeStreamRegistry.ids());
        long removed = coordinator.deleteOrphans(keep);
        if (removed > 0)
            logger.info("Removed " + removed + " orphaned coordination document(s)");
    }

    private void start(ChangeStreamConfig config) {
        try {
            ChangeStreamListener<Document> listener = this.resolveListener(config.getListener());
            ChangeStreamRuntime<Document> runtime = ChangeStreamRuntime.<Document>builder()
                    .collectionName(config.getCollectionName())
                    .listener(listener)
                    .changeStream(config.toChangeStream())
                    .build();
            this.changeStreamService.start(runtime);
            runtime.setConfig(config);
            logger.info("Change stream " + config.getId() + " initiated from config");
        } catch (RuntimeException e) {
            logger.error("Unable to start change stream from config " + config.getId() + ":", e);
        }
    }

    private void stop(String streamId) {
        // deregister from the map first, so reconcile requests queued by the
        // coordination events of the stop itself cannot resurrect the stream
        ChangeStreamRuntime<Document> runtime = this.changeStreamRegistry.deregister(streamId);
        if (runtime != null) {
            try {
                this.changeStreamService.stop(runtime);
            } catch (RuntimeException e) {
                logger.error("Unable to stop change stream " + streamId + ":", e);
            }
        }
    }

    /**
     * Returns the status of every registered change stream, including the
     * coordination stream, config-driven streams and programmatically started
     * ones.
     */
    public List<ChangeStreamStatus> getChangeStreams() {
        return this.changeStreamRegistry.<Document>all().stream().map(this::toStatus).toList();
    }

    /**
     * Returns the status of the change streams currently running on this
     * instance.
     */
    public List<ChangeStreamStatus> getActiveChangeStreams() {
        return this.changeStreamRegistry.<Document>all().stream()
                .filter(runtime -> runtime.getChangeStream() != null && runtime.getChangeStream().isRunning())
                .map(this::toStatus)
                .toList();
    }

    /**
     * Returns the status of the change stream with the given id, if registered.
     */
    public Optional<ChangeStreamStatus> getChangeStreamStatus(String streamId) {
        return Optional.<ChangeStreamRuntime<Document>>ofNullable(this.changeStreamRegistry.get(streamId)).map(this::toStatus);
    }

    private ChangeStreamStatus toStatus(ChangeStreamRuntime<Document> runtime) {
        ChangeStream<Document> stream = runtime.getChangeStream();
        return ChangeStreamStatus.builder()
                .id(stream.getId())
                .collectionName(runtime.getCollectionName())
                .mode(stream.getMode())
                .resumeStrategy(stream.getResumeStrategy())
                .running(stream.isRunning())
                .leader(runtime.getLeader())
                .leaseUntil(runtime.getLeaseUntil())
                .term(runtime.getTerm())
                .instances(runtime.getInstances() == null ? List.of() : List.copyOf(runtime.getInstances()))
                .epoch(runtime.getEpoch())
                .partitionIndex(runtime.getPartitionIndex())
                .partitionCount(runtime.getPartitionCount())
                .resumeToken(stream.getResumeToken())
                .listener(runtime.getListener() == null ? null : runtime.getListener().getClass().getSimpleName())
                .managedByConfig(runtime.getConfig() != null)
                .build();
    }

    @SuppressWarnings("unchecked")
    private ChangeStreamListener<Document> resolveListener(String beanName) {
        if (beanName == null || beanName.isBlank())
            throw new IllegalArgumentException("Change stream config requires a listener bean name");
        try {
            return this.context.getBean(beanName, ChangeStreamListener.class);
        } catch (BeansException e) {
            throw new IllegalArgumentException("No ChangeStreamListener bean named '" + beanName + "' found", e);
        }
    }

    @PreDestroy
    private void clear() {
        for (ChangeStreamRuntime<Document> runtime : this.changeStreamRegistry.<Document>all()) {
            if (runtime.getChangeStream() != null)
                runtime.getChangeStream().setRunning(false);
        }
        this.coordinationStreamStarted = false;
    }

}
