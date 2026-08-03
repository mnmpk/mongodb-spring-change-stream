package com.mzinx.mongodb.changestream.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mzinx.mongodb.changestream.listener.ChangeStreamListener;

import lombok.Data;

@Data
public class ChangeStream<T> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public enum Mode {
        /** Every registered member runs the full stream. */
        BROADCAST,
        /** Exactly one leader runs the stream, with automatic failover. */
        AUTO_RECOVER,
        /** Every member runs a disjoint hash partition of the stream. */
        AUTO_SCALE
    }

    public enum ResumeStrategy {
        /** Checkpoint the resume token after every event. */
        PER_EVENT,
        /** Checkpoint the resume token after each drained batch. */
        PER_BATCH,
        /** Checkpoint the resume token at a fixed interval ({@code checkpointInterval}). */
        INTERVAL,
        /** No resume token management; streams start from now on restart. */
        NONE
    }

    private static final Long DEFAULT_CHECKPOINT_INTERVAL = 60 * 1000l;

    private String id;
    private Mode mode;
    private Integer batchSize;
    private Long maxAwaitTime;
    private ResumeStrategy resumeStrategy = ResumeStrategy.NONE;
    private FullDocumentBeforeChange fullDocumentBeforeChange;
    private FullDocument fullDocument;
    private List<Bson> pipeline = new ArrayList<>();
    private Class<T> documentClass;

    /**
     * Free-form, listener-defined settings carried from the originating
     * {@link ChangeStreamConfig#getAttributes()}. Snapshotted at build time and
     * passed to the listener on every event, so a listener needs no per-event
     * database lookup to read its configuration.
     */
    private Map<String, Object> attributes;

    /** The configured driver iterable the cursor is opened from. */
    private ChangeStreamIterable<T> iterable;
    private MongoChangeStreamCursor<ChangeStreamDocument<T>> cursor;
    private boolean running = false;
    private Long checkpointInterval = DEFAULT_CHECKPOINT_INTERVAL;
    private String resumeToken;
    private ChangeStreamListener<T> listener;

    public ChangeStream(String id, Mode mode, Integer batchSize, Long maxAwaitTime,
            ResumeStrategy resumeStrategy, long checkpointInterval, FullDocumentBeforeChange fullDocumentBeforeChange,
            FullDocument fullDocument, List<Bson> pipeline, Class<T> clazz) {
        this.id = id;
        this.mode = mode;
        this.batchSize = batchSize;
        this.maxAwaitTime = maxAwaitTime;
        this.resumeStrategy = resumeStrategy;
        this.checkpointInterval = checkpointInterval;
        this.fullDocumentBeforeChange = fullDocumentBeforeChange;
        this.fullDocument = fullDocument;
        this.pipeline.clear();
        this.pipeline.addAll(pipeline);
        this.documentClass = clazz;
    }

    public synchronized boolean isRunning() {
        return this.running;
    }

    public synchronized void setRunning(boolean running) {
        this.running = running;
    }

    /**
     * Atomically claims the run flag. The claimer owns the stream life cycle:
     * {@link #watch} only loops while the flag stays set and never sets it
     * itself, so a stop requested between claiming and the watch task actually
     * starting is honored (the loop exits immediately instead of resurrecting
     * the stream).
     *
     * @return {@code true} when the flag was claimed by this call,
     *         {@code false} when the stream was already claimed/running
     */
    public synchronized boolean claim() {
        if (this.running)
            return false;
        this.running = true;
        return true;
    }

    public void watch(MongoCollection<?> coll, ChangeStreamRuntime<T> runtime,
            Consumer<BsonString> checkpoint) {
        this.iterable = coll.watch(getScaledPipeline(runtime), this.documentClass);
        this.watch(runtime.getListener(), checkpoint);
    }

    public void watch(MongoDatabase db, ChangeStreamRuntime<T> runtime, Consumer<BsonString> checkpoint) {
        this.iterable = db.watch(getScaledPipeline(runtime), this.documentClass);
        this.watch(runtime.getListener(), checkpoint);
    }

    /**
     * Runs the blocking watch loop. The run flag must have been claimed by the
     * caller through {@link #claim()} beforehand; if a stop was requested in
     * the meantime ({@link #setRunning setRunning(false)}), the loop exits
     * immediately without opening a cursor.
     */
    public void watch(ChangeStreamListener<T> listener, Consumer<BsonString> checkpoint) {
        logger.info("Initializing change stream " + this.getId());

        if (!this.isRunning()) {
            logger.info("Change stream " + this.getId() + " was stopped before starting");
            return;
        }
        this.listener = listener;
        if (this.batchSize != null) {
            this.iterable = this.iterable.batchSize(this.batchSize);
        }
        if (this.maxAwaitTime != null) {
            this.iterable = this.iterable.maxAwaitTime(this.maxAwaitTime, TimeUnit.MILLISECONDS);
        }
        if (resumeToken != null) {
            // startAfter (not resumeAfter) so the stream also resumes across an
            // invalidate notification, e.g. a checkpoint taken while the watched
            // collection or database was dropped (resumeAfter would fail with
            // error 260 InvalidResumeToken); for regular tokens both behave the
            // same.
            this.iterable = this.iterable
                    .startAfter(new Document("_data", resumeToken).toBsonDocument());
        }
        if (fullDocument != null) {
            this.iterable = this.iterable.fullDocument(fullDocument);
        }
        if (fullDocumentBeforeChange != null) {
            this.iterable = this.iterable.fullDocumentBeforeChange(fullDocumentBeforeChange);
        }
        // Invalid resume tokens (out of oplog window: ChangeStreamHistoryLost 286;
        // token not part of this stream's series, e.g. checkpoints of a differently
        // partitioned AUTO_SCALE pipeline: ChangeStreamFatalError 280 /
        // NonResumableChangeStreamError; unusable token: InvalidResumeToken 260)
        // are recovered by ChangeStreamService: the failure propagates from here,
        // the poisoned checkpoint is discarded and the stream is restarted
        // without it.
        this.cursor = this.iterable.cursor();
        ScheduledExecutorService scheduler = null;
        if (ResumeStrategy.INTERVAL == this.getResumeStrategy()) {
            scheduler = this.scheduleCheckpointTimer(this, checkpoint);
        }

        try {
            while (this.isRunning()) {
                ChangeStreamDocument<T> event = this.getCursor().tryNext();
                if (event != null) {
                    this.getListener().onEvent(this.getId(), this.getAttributes(), event);
                    if ((ResumeStrategy.PER_BATCH == this.getResumeStrategy() && this.getCursor().available() == 0)
                            || ResumeStrategy.PER_EVENT == this.getResumeStrategy()) {
                        checkpoint.accept(event.getResumeToken().getString("_data"));
                    }
                }
            }
        } finally {
            logger.info("Change stream " + this.getId() + " stopped");
            this.setRunning(false);
            if (scheduler != null)
                scheduler.shutdown();
            try {
                this.cursor.close();
            } catch (RuntimeException e) {
                logger.debug("Error closing change stream cursor", e);
            }
        }
    }

    private List<Bson> getScaledPipeline(ChangeStreamRuntime<T> runtime) {
        List<Bson> list = new ArrayList<>(this.pipeline);
        if (Mode.AUTO_SCALE == runtime.getChangeStream().getMode() && runtime.getPartitionCount() > 0
                && runtime.getPartitionIndex() >= 0) {
            list.add(new Document("$match",
                    new Document("$expr",
                            new Document("$eq", Arrays.asList(new Document("$abs",
                                    new Document("$mod", Arrays.asList(
                                            new Document("$toHashedIndexKey", "$documentKey._id"),
                                            runtime.getPartitionCount()))),
                                    runtime.getPartitionIndex()))))
                    .toBsonDocument());
        }
        return list;
    }

    private ScheduledExecutorService scheduleCheckpointTimer(ChangeStream<T> stream, Consumer<BsonString> checkpoint) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    if (stream.isRunning()) {
                        if (stream.getCursor().getResumeToken() != null)
                            checkpoint.accept(stream.getCursor().getResumeToken().getString("_data"));
                    } else {
                        scheduler.shutdown();
                        logger.info("Checkpoint timer stopped");
                    }
                } catch (MongoWriteException e) {
                    if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                        logger.info("Repeated resume token");
                    } else {
                        logger.error("Unable to save resume token", e);
                    }
                } catch (Exception e) {
                    scheduler.shutdown();
                    logger.error("Unexpected error:", e);
                    stream.setRunning(false);
                    throw e;
                }
            }
        };
        logger.info("Start checkpoint timer: " + stream.getCheckpointInterval());
        scheduler.scheduleAtFixedRate(task, 0, stream.getCheckpointInterval(), TimeUnit.MILLISECONDS);
        return scheduler;
    }

    public static ChangeStream<Document> of(String id) {
        return of(id, Mode.BROADCAST);
    }

    public static ChangeStream<Document> of(String id, Mode mode) {
        return of(id, mode, null);
    }

    public static ChangeStream<Document> of(String id, Mode mode,
            List<Bson> pipeline) {
        return new ChangeStream<Document>(id, mode, null, null, ResumeStrategy.NONE, DEFAULT_CHECKPOINT_INTERVAL, null,
                null,
                pipeline,
                Document.class);
    }

    /** Copies the non-constructor metadata ({@code attributes}) onto a derived stream. */
    private <U> ChangeStream<U> withMeta(ChangeStream<U> copy) {
        copy.setAttributes(this.attributes);
        return copy;
    }

    public ChangeStream<T> batchSize(Integer batchSize) {
        return withMeta(new ChangeStream<T>(this.id, this.mode, batchSize, this.maxAwaitTime,
                this.resumeStrategy, this.checkpointInterval, this.fullDocumentBeforeChange, this.fullDocument,
                this.pipeline, this.documentClass));
    }

    public ChangeStream<T> maxAwaitTime(Long maxAwaitTime) {
        return withMeta(new ChangeStream<T>(this.id, this.mode, this.batchSize, maxAwaitTime,
                this.resumeStrategy, this.checkpointInterval, this.fullDocumentBeforeChange, this.fullDocument,
                this.pipeline, this.documentClass));
    }

    public ChangeStream<T> resumeStrategy(ResumeStrategy resumeStrategy) {
        return withMeta(new ChangeStream<T>(this.id, this.mode, this.batchSize, maxAwaitTime,
                resumeStrategy, this.checkpointInterval, this.fullDocumentBeforeChange, this.fullDocument, this.pipeline,
                this.documentClass));
    }

    public ChangeStream<T> resumeStrategy(ResumeStrategy resumeStrategy, long checkpointInterval) {
        return withMeta(new ChangeStream<T>(this.id, this.mode, this.batchSize, maxAwaitTime,
                resumeStrategy, checkpointInterval, this.fullDocumentBeforeChange, this.fullDocument, this.pipeline,
                this.documentClass));
    }

    /**
     * Resumes the stream from the given checkpoint token. Applied with the
     * driver's {@code startAfter}, so resuming across an invalidate event
     * (e.g. a dropped collection) is supported.
     */
    public ChangeStream<T> resumeFrom(String resumeToken) {
        this.resumeToken = resumeToken;
        return this;
    }

    public ChangeStream<T> fullDocumentBeforeChange(FullDocumentBeforeChange fullDocumentBeforeChange) {
        return withMeta(new ChangeStream<T>(this.id, this.mode, this.batchSize, maxAwaitTime,
                this.resumeStrategy, this.checkpointInterval, fullDocumentBeforeChange, this.fullDocument, this.pipeline,
                this.documentClass));
    }

    public ChangeStream<T> fullDocument(FullDocument fullDocument) {
        return withMeta(new ChangeStream<T>(this.id, this.mode, this.batchSize, maxAwaitTime,
                this.resumeStrategy, this.checkpointInterval, this.fullDocumentBeforeChange, fullDocument, this.pipeline,
                this.documentClass));
    }

    public <NewT> ChangeStream<NewT> withClass(Class<NewT> clazz) {
        return withMeta(new ChangeStream<NewT>(this.id, this.mode, this.batchSize, this.maxAwaitTime,
                this.resumeStrategy, this.checkpointInterval, this.fullDocumentBeforeChange, this.fullDocument,
                this.pipeline, clazz));
    }

}
