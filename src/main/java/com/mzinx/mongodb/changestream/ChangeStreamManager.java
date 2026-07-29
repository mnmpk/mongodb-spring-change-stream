package com.mzinx.mongodb.changestream;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import com.mzinx.mongodb.changestream.model.ChangeStreamRegistry;
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
 * <li><b>reconcileAll</b> — synchronizes every local registry from its
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
    Logger logger = LoggerFactory.getLogger(getClass());

    private static final String COORDINATION_STREAM_ID = "change-stream";

    private final ApplicationContext context;
    private final ChangeStreamProperties changeStreamProperties;
    private final ChangeStreamService<Document> changeStreamService;
    private final ChangeStreamConfigService changeStreamConfigService;
    private final ChangeStreamCoordinator coordinator;

    /**
     * All change stream registries, keyed by change stream id. This is the shared
     * registry map bean also populated by {@link ChangeStreamService} for streams
     * started programmatically, so the manager oversees every registry, including
     * the coordination stream. Config-driven registries carry the applied config
     * in {@link ChangeStreamRegistry#getConfig()}.
     */
    private final Map<String, ChangeStreamRegistry<Document>> changeStreams;

    private volatile boolean coordinating = false;

    ChangeStreamManager(ApplicationContext context, ChangeStreamProperties changeStreamProperties,
            ChangeStreamService<Document> changeStreamService, ChangeStreamConfigService changeStreamConfigService,
            ChangeStreamCoordinator coordinator, Map<String, ChangeStreamRegistry<Document>> changeStreams) {
        this.context = context;
        this.changeStreamProperties = changeStreamProperties;
        this.changeStreamService = changeStreamService;
        this.changeStreamConfigService = changeStreamConfigService;
        this.coordinator = coordinator;
        this.changeStreams = changeStreams;
    }

    @Scheduled(initialDelayString = "${change-stream.config-refresh-initial-delay:10000}", fixedDelayString = "${change-stream.config-refresh-interval:30000}")
    void watch() {
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
     * nodes with low latency. Its registry is kept in the shared registry map
     * like any other change stream and reconciled by the same loop.
     */
    private void coordinate() {
        if (this.coordinating)
            return;
        try {
            ChangeStreamListener<Document> changeStreamWatch = this.resolveListener("changeStreamWatch");
            ChangeStreamRegistry<Document> coordinationRegistry = ChangeStreamRegistry.<Document>builder()
                    .collectionName(changeStreamProperties.getChangeStreamCollection())
                    .listener(changeStreamWatch)
                    .changeStream(ChangeStream.of(COORDINATION_STREAM_ID, Mode.BOARDCAST,
                            List.of(Aggregates.match(
                                    Filters.in("operationType", List.of("insert", "update", "delete"))))))
                    .build();
            this.changeStreamService.start(coordinationRegistry);
            this.coordinating = true;
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
            ChangeStreamRegistry<Document> reg = this.changeStreams.get(config.getId());
            // config on the registry is only set after a successful start, so a
            // half-started registry is retried on the next refresh
            ChangeStreamConfig current = reg == null ? null : reg.getConfig();

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
        for (Map.Entry<String, ChangeStreamRegistry<Document>> entry : Set.copyOf(this.changeStreams.entrySet())) {
            if (entry.getValue().getConfig() != null && !known.contains(entry.getKey())) {
                logger.info("Change stream config " + entry.getKey() + " removed, stopping");
                this.stop(entry.getKey());
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
        for (ChangeStreamRegistry<Document> reg : List.copyOf(this.changeStreams.values())) {
            try {
                this.changeStreamService.reconcile(reg);
            } catch (RuntimeException e) {
                logger.error("Unable to reconcile change stream "
                        + reg.getChangeStream().getId() + ":", e);
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
        keep.addAll(this.changeStreams.keySet());
        long removed = coordinator.deleteOrphans(keep);
        if (removed > 0)
            logger.info("Removed " + removed + " orphaned coordination document(s)");
    }

    private void start(ChangeStreamConfig config) {
        try {
            ChangeStreamListener<Document> listener = this.resolveListener(config.getListener());
            ChangeStreamRegistry<Document> reg = ChangeStreamRegistry.<Document>builder()
                    .collectionName(config.getCollectionName())
                    .listener(listener)
                    .changeStream(config.toChangeStream())
                    .build();
            this.changeStreamService.start(reg);
            reg.setConfig(config);
            logger.info("Change stream " + config.getId() + " initiated from config");
        } catch (RuntimeException e) {
            logger.error("Unable to start change stream from config " + config.getId() + ":", e);
        }
    }

    private void stop(String csId) {
        // deregister from the map first, so reconcile requests queued by the
        // coordination events of the stop itself cannot resurrect the stream
        ChangeStreamRegistry<Document> reg = this.changeStreams.remove(csId);
        if (reg != null) {
            try {
                this.changeStreamService.stop(reg, false);
            } catch (RuntimeException e) {
                logger.error("Unable to stop change stream " + csId + ":", e);
            }
        }
    }

    /**
     * Returns the status of every registered change stream, including the
     * coordination stream, config-driven streams and programmatically started
     * ones.
     */
    public List<ChangeStreamStatus> getChangeStreams() {
        return this.changeStreams.values().stream().map(this::toStatus).toList();
    }

    /**
     * Returns the status of the change streams currently running on this
     * instance.
     */
    public List<ChangeStreamStatus> getActiveChangeStreams() {
        return this.changeStreams.values().stream()
                .filter(reg -> reg.getChangeStream() != null && reg.getChangeStream().isRunning())
                .map(this::toStatus)
                .toList();
    }

    /**
     * Returns the status of the change stream with the given id, if registered.
     */
    public Optional<ChangeStreamStatus> getChangeStreamStatus(String csId) {
        return Optional.ofNullable(this.changeStreams.get(csId)).map(this::toStatus);
    }

    private ChangeStreamStatus toStatus(ChangeStreamRegistry<Document> reg) {
        ChangeStream<Document> cs = reg.getChangeStream();
        return ChangeStreamStatus.builder()
                .id(cs.getId())
                .collectionName(reg.getCollectionName())
                .mode(cs.getMode())
                .resumeStrategy(cs.getResumeStrategy())
                .running(cs.isRunning())
                .leader(reg.getLeader())
                .leaseUntil(reg.getLeaseUntil())
                .term(reg.getTerm())
                .instances(reg.getInstances() == null ? List.of() : List.copyOf(reg.getInstances()))
                .epoch(reg.getEpoch())
                .instanceIndex(reg.getInstanceIndex())
                .instanceSize(reg.getInstanceSize())
                .resumeToken(cs.getResumeToken())
                .listener(reg.getListener() == null ? null : reg.getListener().getClass().getSimpleName())
                .managedByConfig(reg.getConfig() != null)
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
        for (ChangeStreamRegistry<Document> reg : this.changeStreams.values()) {
            if (reg.getChangeStream() != null)
                reg.getChangeStream().setRunning(false);
        }
        this.coordinating = false;
    }

}
