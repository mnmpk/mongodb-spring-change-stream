package com.mzinx.mongodb.changestream.bootstrap;

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
import com.mzinx.mongodb.changestream.service.ChangeStreamService;

import jakarta.annotation.PreDestroy;

/**
 * Periodically fetches {@link ChangeStreamConfig} documents from the config
 * collection and manages the life cycle of the corresponding change streams:
 * <ul>
 * <li>starts change streams for new enabled configs</li>
 * <li>restarts change streams whose config definition changed</li>
 * <li>stops change streams whose config was removed or disabled</li>
 * </ul>
 * All change stream registries (the coordination stream, config-driven streams
 * and streams started programmatically through
 * {@link ChangeStreamService#start(ChangeStreamRegistry)}) are managed through
 * the shared registry map, and their runtime status can be queried via
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
            Map<String, ChangeStreamRegistry<Document>> changeStreams) {
        this.context = context;
        this.changeStreamProperties = changeStreamProperties;
        this.changeStreamService = changeStreamService;
        this.changeStreamConfigService = changeStreamConfigService;
        this.changeStreams = changeStreams;
    }

    @Scheduled(initialDelayString = "${change-stream.config-refresh-initial-delay:10000}", fixedDelayString = "${change-stream.config-refresh-interval:30000}")
    void watch() {
        this.coordinate();
        try {
            this.refresh();
        } catch (RuntimeException e) {
            logger.error("Unable to refresh change stream configs:", e);
        }
    }

    /**
     * Starts the coordination change stream watching the change stream
     * coordination collection, so leader/instance changes are propagated to all
     * nodes. Its registry is kept in the shared registry map like any other
     * change stream.
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
     * Fetches change stream configs from the config collection and reconciles
     * them with the currently managed change streams.
     */
    private void refresh() {
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
        ChangeStreamRegistry<Document> reg = this.changeStreams.get(csId);
        if (reg != null) {
            try {
                this.changeStreamService.stop(reg, false);
            } catch (RuntimeException e) {
                logger.error("Unable to stop change stream " + csId + ":", e);
            }
            this.changeStreams.remove(csId);
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
                .instances(reg.getInstances() == null ? List.of() : List.copyOf(reg.getInstances()))
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
