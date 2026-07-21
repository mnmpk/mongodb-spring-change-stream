package com.mzinx.mongodb.changestream.bootstrap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
 */
@Component
public class ChangeStreamManager {
    Logger logger = LoggerFactory.getLogger(getClass());

    private static final String COORDINATION_STREAM_ID = "change-stream";

    private final ApplicationContext context;
    private final ChangeStreamProperties changeStreamProperties;
    private final ChangeStreamService<Document> changeStreamService;
    private final ChangeStreamConfigService changeStreamConfigService;

    /** Change streams managed by this instance, keyed by config id. */
    private final Map<String, ManagedChangeStream> managed = new ConcurrentHashMap<>();

    private volatile boolean coordinating = false;
    private ChangeStreamRegistry<Document> coordinationRegistry;

    ChangeStreamManager(ApplicationContext context, ChangeStreamProperties changeStreamProperties,
            ChangeStreamService<Document> changeStreamService, ChangeStreamConfigService changeStreamConfigService) {
        this.context = context;
        this.changeStreamProperties = changeStreamProperties;
        this.changeStreamService = changeStreamService;
        this.changeStreamConfigService = changeStreamConfigService;
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
     * nodes.
     */
    private void coordinate() {
        if (this.coordinating)
            return;
        try {
            ChangeStreamListener<Document> changeStreamWatch = this.resolveListener("changeStreamWatch");
            this.coordinationRegistry = ChangeStreamRegistry.<Document>builder()
                    .collectionName(changeStreamProperties.getChangeStreamCollection())
                    .listener(changeStreamWatch)
                    .changeStream(ChangeStream.of(COORDINATION_STREAM_ID, Mode.BOARDCAST,
                            List.of(Aggregates.match(
                                    Filters.in("operationType", List.of("insert", "update", "delete"))))))
                    .build();
            this.changeStreamService.start(this.coordinationRegistry);
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
            known.add(config.getId());
            ManagedChangeStream current = this.managed.get(config.getId());

            if (!config.isEnabled()) {
                if (current != null) {
                    logger.info("Change stream config " + config.getId() + " disabled, stopping");
                    this.stop(current);
                    this.managed.remove(config.getId());
                }
                continue;
            }

            if (current == null) {
                this.start(config);
            } else if (!current.config().isSameDefinition(config)) {
                logger.info("Change stream config " + config.getId() + " changed, restarting");
                this.stop(current);
                this.managed.remove(config.getId());
                this.start(config);
            }
        }

        // stop change streams whose config was removed
        Iterator<Map.Entry<String, ManagedChangeStream>> it = this.managed.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ManagedChangeStream> entry = it.next();
            if (!known.contains(entry.getKey())) {
                logger.info("Change stream config " + entry.getKey() + " removed, stopping");
                this.stop(entry.getValue());
                it.remove();
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
            this.managed.put(config.getId(), new ManagedChangeStream(config, reg));
            logger.info("Change stream " + config.getId() + " initiated from config");
        } catch (RuntimeException e) {
            logger.error("Unable to start change stream from config " + config.getId() + ":", e);
        }
    }

    private void stop(ManagedChangeStream ms) {
        try {
            this.changeStreamService.stop(ms.registry(), false);
        } catch (RuntimeException e) {
            logger.error("Unable to stop change stream " + ms.config().getId() + ":", e);
        }
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
        for (ManagedChangeStream ms : this.managed.values()) {
            ms.registry().getChangeStream().setRunning(false);
        }
        this.managed.clear();
        if (this.coordinationRegistry != null) {
            this.coordinationRegistry.getChangeStream().setRunning(false);
        }
    }

    private record ManagedChangeStream(ChangeStreamConfig config, ChangeStreamRegistry<Document> registry) {
    }

}
