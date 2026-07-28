package com.mzinx.changestreamtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mzinx.mongodb.changestream.bootstrap.ChangeStreamManager;
import com.mzinx.mongodb.changestream.config.ChangeStreamProperties;
import com.mzinx.mongodb.changestream.model.ChangeStream.Mode;
import com.mzinx.mongodb.changestream.model.ChangeStreamConfig;
import com.mzinx.mongodb.changestream.model.ChangeStreamStatus;
import com.mzinx.mongodb.changestream.service.ChangeStreamConfigService;

/**
 * Functional tests running against a real MongoDB Atlas cluster.
 * <p>
 * The Atlas connection string must be provided through the
 * {@value #CONNECTION_STRING_ENV} environment variable, e.g.
 * {@code MONGODB_URI="mongodb+srv://user:password@host/"}. When the variable
 * is not set, the tests are skipped.
 * <p>
 * They verify that {@link ChangeStreamManager} manages every change stream
 * registry (the coordination stream and config-driven streams) and that its
 * status query interface reflects the actual runtime state.
 * <p>
 * Tests are ordered and share the Spring context; a dedicated test database is
 * dropped before the context starts so every run begins from a clean state.
 */
@SpringBootTest(classes = TestApplication.class, properties = {
        "change-stream.config-refresh-initial-delay=500",
        "change-stream.config-refresh-interval=1500"
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ChangeStreamManagerFunctionalTest {

    static final String CONNECTION_STRING_ENV = "MONGODB_URI";
    static final String DATABASE = "change_stream_functional_test";

    private static final String COORDINATION_STREAM_ID = "change-stream";
    private static final String CONFIG_STREAM_ID = "orders-functional-stream";
    private static final String REMOVED_CONFIG_STREAM_ID = "orders-functional-stream-removed";
    private static final String WATCHED_COLLECTION = "orders_functional_test";
    private static final String RUN_ID = UUID.randomUUID().toString();

    private static final long START_TIMEOUT_MS = 90_000;
    private static final long STOP_TIMEOUT_MS = 60_000;
    private static final long POLL_INTERVAL_MS = 500;

    @Autowired
    private ChangeStreamManager changeStreamManager;
    @Autowired
    private ChangeStreamConfigService changeStreamConfigService;
    @Autowired
    private ChangeStreamProperties changeStreamProperties;
    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private TestRecordingListener testChangeStreamListener;

    /**
     * Resolves the Atlas connection string from the {@value #CONNECTION_STRING_ENV}
     * environment variable, skipping the tests when it is not set.
     */
    private static String connectionString() {
        String connectionString = System.getenv(CONNECTION_STRING_ENV);
        assumeTrue(connectionString != null && !connectionString.isBlank(),
                "Skipping functional tests: " + CONNECTION_STRING_ENV + " environment variable is not set");
        return connectionString.endsWith("/") ? connectionString : connectionString + "/";
    }

    /**
     * Points the MongoDB clients at the Atlas test database resolved from the
     * environment.
     */
    @DynamicPropertySource
    static void mongoUri(DynamicPropertyRegistry registry) {
        String uri = connectionString() + DATABASE;
        // Spring Boot 4 property prefix
        registry.add("spring.mongodb.uri", () -> uri);
        // pre-Boot-4 property prefix, kept for compatibility
        registry.add("spring.data.mongodb.uri", () -> uri);
    }

    /**
     * Drops the test database before the Spring context (and therefore the
     * coordination stream) starts, so each run begins from a clean state.
     */
    @BeforeAll
    static void cleanDatabase() {
        try (MongoClient client = MongoClients.create(connectionString() + DATABASE)) {
            client.getDatabase(DATABASE).drop();
        }
    }

    @Test
    @Order(1)
    void coordinationStreamIsManagedAndQueryable() {
        await("coordination stream to start", START_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(COORDINATION_STREAM_ID)
                        .map(ChangeStreamStatus::isRunning).orElse(false));

        ChangeStreamStatus status = this.changeStreamManager.getChangeStreamStatus(COORDINATION_STREAM_ID)
                .orElseThrow();
        assertTrue(status.isRunning());
        assertEquals(Mode.BOARDCAST, status.getMode());
        assertEquals(this.changeStreamProperties.getChangeStreamCollection(), status.getCollectionName());
        assertEquals(this.changeStreamProperties.getHostname(), status.getLeader());
        assertTrue(status.getInstances().contains(this.changeStreamProperties.getHostname()));
        assertFalse(status.isManagedByConfig(), "coordination stream is internal, not config driven");
        assertEquals("ChangeStreamWatch", status.getListener());
    }

    @Test
    @Order(2)
    void configDrivenStreamStartsAndDeliversEvents() {
        this.changeStreamConfigService.save(ChangeStreamConfig.builder()
                .id(CONFIG_STREAM_ID)
                .collectionName(WATCHED_COLLECTION)
                .mode(Mode.BOARDCAST)
                .listener("testChangeStreamListener")
                .build());

        await("config-driven stream to start", START_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(CONFIG_STREAM_ID)
                        .map(ChangeStreamStatus::isRunning).orElse(false));

        ChangeStreamStatus status = this.changeStreamManager.getChangeStreamStatus(CONFIG_STREAM_ID).orElseThrow();
        assertTrue(status.isRunning());
        assertEquals(WATCHED_COLLECTION, status.getCollectionName());
        assertEquals(Mode.BOARDCAST, status.getMode());
        assertEquals(this.changeStreamProperties.getHostname(), status.getLeader());
        assertTrue(status.isManagedByConfig());
        assertEquals("TestRecordingListener", status.getListener());

        // keep inserting marker documents until one is captured by the listener,
        // to avoid racing the change stream cursor creation
        await("insert event to reach the test listener", START_TIMEOUT_MS, () -> {
            if (this.testChangeStreamListener.hasEvent(
                    e -> e.getFullDocument() != null && RUN_ID.equals(e.getFullDocument().getString("runId"))))
                return true;
            this.mongoTemplate.getCollection(WATCHED_COLLECTION).insertOne(new Document("runId", RUN_ID));
            return false;
        });
    }

    @Test
    @Order(3)
    void statusQueryInterfaceReflectsAllRegistries() {
        List<ChangeStreamStatus> all = this.changeStreamManager.getChangeStreams();
        assertTrue(all.stream().anyMatch(s -> COORDINATION_STREAM_ID.equals(s.getId())),
                "coordination stream should be managed alongside the other registries");
        assertTrue(all.stream().anyMatch(s -> CONFIG_STREAM_ID.equals(s.getId())));

        List<ChangeStreamStatus> active = this.changeStreamManager.getActiveChangeStreams();
        assertTrue(active.stream().allMatch(ChangeStreamStatus::isRunning));
        assertTrue(active.stream().anyMatch(s -> COORDINATION_STREAM_ID.equals(s.getId())));
        assertTrue(active.stream().anyMatch(s -> CONFIG_STREAM_ID.equals(s.getId())));

        Optional<ChangeStreamStatus> unknown = this.changeStreamManager.getChangeStreamStatus("does-not-exist");
        assertTrue(unknown.isEmpty());
    }

    @Test
    @Order(4)
    void disabledConfigStopsAndDeregistersStream() {
        ChangeStreamConfig config = this.changeStreamConfigService.findById(CONFIG_STREAM_ID);
        config.setEnabled(false);
        this.changeStreamConfigService.save(config);

        await("disabled stream to stop and deregister", STOP_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(CONFIG_STREAM_ID).isEmpty());

        assertTrue(this.changeStreamManager.getActiveChangeStreams().stream()
                .noneMatch(s -> CONFIG_STREAM_ID.equals(s.getId())));
        // this host must have been deregistered from the coordination document
        Document coordinationDoc = this.mongoTemplate
                .getCollection(this.changeStreamProperties.getChangeStreamCollection())
                .find(new Document("_id", CONFIG_STREAM_ID)).first();
        assertFalse(coordinationDoc.getList("i", String.class).contains(this.changeStreamProperties.getHostname()),
                "host should be deregistered from the coordination document");
        // the coordination stream must not be affected
        assertTrue(this.changeStreamManager.getChangeStreamStatus(COORDINATION_STREAM_ID)
                .map(ChangeStreamStatus::isRunning).orElse(false));
    }

    @Test
    @Order(5)
    void removedConfigStopsAndDeregistersStream() {
        this.changeStreamConfigService.save(ChangeStreamConfig.builder()
                .id(REMOVED_CONFIG_STREAM_ID)
                .collectionName(WATCHED_COLLECTION)
                .mode(Mode.BOARDCAST)
                .listener("testChangeStreamListener")
                .build());

        await("stream to start before config removal", START_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(REMOVED_CONFIG_STREAM_ID)
                        .map(ChangeStreamStatus::isRunning).orElse(false));

        this.changeStreamConfigService.delete(REMOVED_CONFIG_STREAM_ID);

        await("removed stream to stop and deregister", STOP_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(REMOVED_CONFIG_STREAM_ID).isEmpty());

        // the coordination stream must still be running
        assertTrue(this.changeStreamManager.getChangeStreamStatus(COORDINATION_STREAM_ID)
                .map(ChangeStreamStatus::isRunning).orElse(false));
    }

    private static void await(String description, long timeoutMillis, BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.getAsBoolean())
                    return;
            } catch (RuntimeException e) {
                // condition not ready yet, keep polling
            }
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for " + description);
            }
        }
        fail("Timed out after " + timeoutMillis + "ms waiting for " + description);
    }
}
