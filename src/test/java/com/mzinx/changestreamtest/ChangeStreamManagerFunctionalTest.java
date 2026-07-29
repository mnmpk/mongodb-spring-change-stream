package com.mzinx.changestreamtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
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
import com.mzinx.mongodb.changestream.ChangeStreamManager;
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
    /**
     * Unique per-run database so concurrent workflow runs (e.g. CI and a
     * release gate) can never interfere with each other on the shared test
     * cluster. Dropped again after the run.
     */
    static final String DATABASE = "cs_func_test_" + UUID.randomUUID().toString().substring(0, 8);

    private static final String COORDINATION_STREAM_ID = "change-stream";
    private static final String CONFIG_STREAM_ID = "orders-functional-stream";
    private static final String REMOVED_CONFIG_STREAM_ID = "orders-functional-stream-removed";
    private static final String RECOVER_STREAM_ID = "orders-functional-stream-recover";
    private static final String SCALE_STREAM_ID = "orders-functional-stream-scale";
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
     * Builds a connection string pointing at the test database from the
     * {@value #CONNECTION_STRING_ENV} environment variable, skipping the tests
     * when it is not set. Tolerates connection strings that already carry a
     * database and/or query options (e.g.
     * {@code mongodb+srv://user:pass@host/mydb?retryWrites=true}): the
     * database path is replaced with {@value #DATABASE} and the options are
     * preserved.
     */
    private static String testDatabaseUri() {
        String raw = System.getenv(CONNECTION_STRING_ENV);
        assumeTrue(raw != null && !raw.isBlank(),
                "Skipping functional tests: " + CONNECTION_STRING_ENV + " environment variable is not set");
        String scheme = raw.startsWith("mongodb+srv://") ? "mongodb+srv://" : "mongodb://";
        String rest = raw.substring(scheme.length());
        String query = "";
        int questionMark = rest.indexOf('?');
        if (questionMark >= 0) {
            query = rest.substring(questionMark);
            rest = rest.substring(0, questionMark);
        }
        int slash = rest.indexOf('/');
        String authority = slash >= 0 ? rest.substring(0, slash) : rest; // drop any database in the URI
        return scheme + authority + "/" + DATABASE + query;
    }

    /**
     * Points the MongoDB clients at the Atlas test database resolved from the
     * environment.
     */
    @DynamicPropertySource
    static void mongoUri(DynamicPropertyRegistry registry) {
        String uri = testDatabaseUri();
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
        try (MongoClient client = MongoClients.create(testDatabaseUri())) {
            client.getDatabase(DATABASE).drop();
        }
    }

    /** Drops the per-run database so test runs leave nothing behind. */
    @AfterAll
    static void dropDatabase() {
        try (MongoClient client = MongoClients.create(testDatabaseUri())) {
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
        assertEquals(Mode.BROADCAST, status.getMode());
        assertEquals(this.changeStreamProperties.getCoordinationCollection(), status.getCollectionName());
        // BROADCAST mode needs no leader: every member runs the full stream
        assertNull(status.getLeader());
        assertTrue(status.getInstances().contains(this.changeStreamProperties.getHostname()));
        assertFalse(status.isManagedByConfig(), "coordination stream is internal, not config driven");
        assertEquals("CoordinationChangeListener", status.getListener());
    }

    @Test
    @Order(2)
    void configDrivenStreamStartsAndDeliversEvents() {
        this.changeStreamConfigService.save(ChangeStreamConfig.builder()
                .id(CONFIG_STREAM_ID)
                .collectionName(WATCHED_COLLECTION)
                .mode(Mode.BROADCAST)
                .listener("testChangeStreamListener")
                .build());

        await("config-driven stream to start", START_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(CONFIG_STREAM_ID)
                        .map(ChangeStreamStatus::isRunning).orElse(false));

        ChangeStreamStatus status = this.changeStreamManager.getChangeStreamStatus(CONFIG_STREAM_ID).orElseThrow();
        assertTrue(status.isRunning());
        assertEquals(WATCHED_COLLECTION, status.getCollectionName());
        assertEquals(Mode.BROADCAST, status.getMode());
        // BROADCAST mode needs no leader: every member runs the full stream
        assertNull(status.getLeader());
        assertTrue(status.getInstances().contains(this.changeStreamProperties.getHostname()));
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
        // this host must be deregistered from the coordination document (the
        // registry disappears from the status API slightly before the
        // deregistration write completes, so poll the document)
        await("host to be deregistered from the coordination document", STOP_TIMEOUT_MS, () -> {
            Document coordinationDoc = this.mongoTemplate
                    .getCollection(this.changeStreamProperties.getCoordinationCollection())
                    .find(new Document("_id", CONFIG_STREAM_ID)).first();
            return coordinationDoc != null && !coordinationDoc.getList("i", String.class)
                    .contains(this.changeStreamProperties.getHostname());
        });
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
                .mode(Mode.BROADCAST)
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

    @Test
    @Order(6)
    void autoRecoverStreamAcquiresLeaderLease() {
        this.changeStreamConfigService.save(ChangeStreamConfig.builder()
                .id(RECOVER_STREAM_ID)
                .collectionName(WATCHED_COLLECTION)
                .mode(Mode.AUTO_RECOVER)
                .listener("testChangeStreamListener")
                .build());

        await("auto-recover stream to acquire the lease and start", START_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(RECOVER_STREAM_ID)
                        .map(ChangeStreamStatus::isRunning).orElse(false));

        ChangeStreamStatus status = this.changeStreamManager.getChangeStreamStatus(RECOVER_STREAM_ID).orElseThrow();
        assertEquals(Mode.AUTO_RECOVER, status.getMode());
        assertEquals(this.changeStreamProperties.getHostname(), status.getLeader());
        assertNotNull(status.getLeaseUntil(), "leader lease must carry a server-time expiry");
        assertTrue(status.getTerm() >= 1, "acquiring leadership must bump the fencing term");
        assertTrue(status.getInstances().contains(this.changeStreamProperties.getHostname()));
    }

    @Test
    @Order(7)
    void autoRecoverStandsByForForeignLeaseAndTakesOverWhenItExpires() {
        long termBefore = this.changeStreamManager.getChangeStreamStatus(RECOVER_STREAM_ID)
                .orElseThrow().getTerm();

        // simulate another instance holding a valid lease: this host must stop
        this.mongoTemplate.getCollection(this.changeStreamProperties.getCoordinationCollection()).updateOne(
                new Document("_id", RECOVER_STREAM_ID),
                new Document("$set", new Document("l", new Document("h", "other-host")
                        .append("until", new Date(System.currentTimeMillis() + 3_600_000)))));

        await("local runner to stand by while another host leads", STOP_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(RECOVER_STREAM_ID)
                        .map(s -> "other-host".equals(s.getLeader()) && !s.isRunning()).orElse(false));

        // expire the foreign lease: this host must take over with a higher term
        this.mongoTemplate.getCollection(this.changeStreamProperties.getCoordinationCollection()).updateOne(
                new Document("_id", RECOVER_STREAM_ID),
                new Document("$set", new Document("l.until", new Date(System.currentTimeMillis() - 1_000))));

        await("local runner to take over the expired lease", START_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(RECOVER_STREAM_ID)
                        .map(s -> this.changeStreamProperties.getHostname().equals(s.getLeader()) && s.isRunning())
                        .orElse(false));

        ChangeStreamStatus status = this.changeStreamManager.getChangeStreamStatus(RECOVER_STREAM_ID).orElseThrow();
        assertTrue(status.getTerm() > termBefore, "taking over leadership must bump the fencing term");
    }

    @Test
    @Order(8)
    void autoScaleStreamPartitionsFromCoordinationDocument() {
        this.changeStreamConfigService.save(ChangeStreamConfig.builder()
                .id(SCALE_STREAM_ID)
                .collectionName(WATCHED_COLLECTION)
                .mode(Mode.AUTO_SCALE)
                .listener("testChangeStreamListener")
                .build());

        await("auto-scale stream to start as the only member", START_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(SCALE_STREAM_ID)
                        .map(s -> s.isRunning() && s.getPartitionCount() == 1).orElse(false));

        ChangeStreamStatus status = this.changeStreamManager.getChangeStreamStatus(SCALE_STREAM_ID).orElseThrow();
        assertEquals(0, status.getPartitionIndex());
        assertTrue(status.getEpoch() >= 1, "first join must bump the membership epoch");

        // simulate a second member joining: bump the epoch, everyone repartitions
        // deterministically from the sorted member list
        String hostname = this.changeStreamProperties.getHostname();
        List<String> members = List.of(hostname, "zzz-fake-host").stream().sorted().toList();
        this.mongoTemplate.getCollection(this.changeStreamProperties.getCoordinationCollection()).updateOne(
                new Document("_id", SCALE_STREAM_ID),
                new Document("$set", new Document("i", members)
                        .append("e", status.getEpoch() + 1)));

        await("auto-scale stream to repartition to 2 members", START_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(SCALE_STREAM_ID)
                        .map(s -> s.isRunning() && s.getPartitionCount() == 2
                                && s.getPartitionIndex() == members.indexOf(hostname))
                        .orElse(false));
    }

    @Test
    @Order(9)
    void registryIsSynchronizedFromCoordinationDocument() {
        String hostname = this.changeStreamProperties.getHostname();
        this.changeStreamConfigService.save(ChangeStreamConfig.builder()
                .id(CONFIG_STREAM_ID + "-sync")
                .collectionName(WATCHED_COLLECTION)
                .mode(Mode.BROADCAST)
                .listener("testChangeStreamListener")
                .build());

        await("stream to start", START_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(CONFIG_STREAM_ID + "-sync")
                        .map(ChangeStreamStatus::isRunning).orElse(false));

        // externally wipe the membership: the reconciler must re-join (repair
        // the document) and re-synchronize the registry cache from it
        this.mongoTemplate.getCollection(this.changeStreamProperties.getCoordinationCollection()).updateOne(
                new Document("_id", CONFIG_STREAM_ID + "-sync"),
                new Document("$set", new Document("i", List.of())));

        await("coordination document to be repaired and registry re-synchronized", START_TIMEOUT_MS, () -> {
            Document doc = this.mongoTemplate.getCollection(this.changeStreamProperties.getCoordinationCollection())
                    .find(new Document("_id", CONFIG_STREAM_ID + "-sync")).first();
            boolean docRepaired = doc != null && doc.getList("i", String.class).contains(hostname);
            boolean registrySynced = this.changeStreamManager.getChangeStreamStatus(CONFIG_STREAM_ID + "-sync")
                    .map(s -> s.isRunning() && s.getInstances().contains(hostname)).orElse(false);
            return docRepaired && registrySynced;
        });
    }

    @Test
    @Order(10)
    void legacyCoordinationDocumentIsMigratedAndTakenOver() {
        String legacyId = "orders-functional-stream-legacy";
        // pre-0.0.7 document shape: leader as plain string, no term/epoch
        this.mongoTemplate.getCollection(this.changeStreamProperties.getCoordinationCollection()).insertOne(
                new Document("_id", legacyId)
                        .append("l", "dead-legacy-host")
                        .append("i", List.of("dead-legacy-host"))
                        .append("at", new Date()));

        this.changeStreamConfigService.save(ChangeStreamConfig.builder()
                .id(legacyId)
                .collectionName(WATCHED_COLLECTION)
                .mode(Mode.AUTO_RECOVER)
                .listener("testChangeStreamListener")
                .build());

        // a legacy string leader parses as an open lease, so this host takes
        // over and the document is lazily migrated to the lease shape
        await("legacy stream to be migrated and taken over", START_TIMEOUT_MS,
                () -> this.changeStreamManager.getChangeStreamStatus(legacyId)
                        .map(s -> s.isRunning()
                                && this.changeStreamProperties.getHostname().equals(s.getLeader())
                                && s.getTerm() >= 1)
                        .orElse(false));

        Document doc = this.mongoTemplate.getCollection(this.changeStreamProperties.getCoordinationCollection())
                .find(new Document("_id", legacyId)).first();
        assertTrue(doc.get("l") instanceof Document, "leader must be migrated to the lease shape");
        assertEquals(this.changeStreamProperties.getHostname(), ((Document) doc.get("l")).getString("h"));
        assertTrue(doc.getList("i", String.class).contains(this.changeStreamProperties.getHostname()));
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
