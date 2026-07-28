package com.mzinx.mongodb.changestream.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    Logger logger = LoggerFactory.getLogger(getClass());

    public enum Mode {
        BOARDCAST,
        AUTO_RECOVER,
        AUTO_SCALE
    }

    public enum ResumeStrategy {
        EVERY,
        BATCH,
        TIME,
        NONE
    }

    private static final Long DEFAULT_SAVE_TOKEN_INTERVAL = 60 * 1000l;

    private String id;
    private Mode mode;
    private Integer batchSize;
    private Long maxAwaitTime;
    private ResumeStrategy resumeStrategy = ResumeStrategy.NONE;
    private FullDocumentBeforeChange fullDocumentBeforeChange;
    private FullDocument fullDocument;
    private List<Bson> pipeline = new ArrayList<>();
    private Class<T> documentClass;

    private ChangeStreamIterable<T> _changeStream;
    private MongoChangeStreamCursor<ChangeStreamDocument<T>> cursor;
    private boolean running = false;
    private Long saveTokenInterval = DEFAULT_SAVE_TOKEN_INTERVAL;
    private String resumeToken;
    private ChangeStreamListener<T> consumer;

    public ChangeStream(String id, Mode mode, Integer batchSize, Long maxAwaitTime,
            ResumeStrategy resumeStrategy, long saveTokenInterval, FullDocumentBeforeChange fullDocumentBeforeChange,
            FullDocument fullDocument, List<Bson> pipeline, Class<T> clazz) {
        this.id = id;
        this.mode = mode;
        this.batchSize = batchSize;
        this.maxAwaitTime = maxAwaitTime;
        this.resumeStrategy = resumeStrategy;
        this.saveTokenInterval = saveTokenInterval;
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

    public void watch(MongoCollection<?> coll, ChangeStreamRegistry<T> reg,
            Consumer<BsonString> checkPoint) {
        this._changeStream = coll.watch(getScaledPipeline(reg), this.documentClass);
        this.watch(reg.getListener(), checkPoint);
    }

    public void watch(MongoDatabase db, ChangeStreamRegistry<T> reg, Consumer<BsonString> checkPoint) {
        this._changeStream = db.watch(getScaledPipeline(reg), this.documentClass);
        this.watch(reg.getListener(), checkPoint);
    }

    /**
     * Runs the blocking watch loop. The run flag must have been claimed by the
     * caller through {@link #claim()} beforehand; if a stop was requested in
     * the meantime ({@link #setRunning setRunning(false)}), the loop exits
     * immediately without opening a cursor.
     */
    public void watch(ChangeStreamListener<T> consumer, Consumer<BsonString> checkPoint) {
        logger.info("Initializing change stream " + this.getId());

        if (!this.isRunning()) {
            logger.info("Change stream " + this.getId() + " was stopped before starting");
            return;
        }
        this.consumer = consumer;
        if (this.batchSize != null) {
            this._changeStream = this._changeStream.batchSize(this.batchSize);
        }
        if (this.maxAwaitTime != null) {
            this._changeStream = this._changeStream.maxAwaitTime(this.maxAwaitTime, TimeUnit.MILLISECONDS);
        }
        if (resumeToken != null) {
            this._changeStream = this._changeStream
                    .resumeAfter(new Document("_data", resumeToken).toBsonDocument());
        }
        if (fullDocument != null) {
            this._changeStream = this._changeStream.fullDocument(fullDocument);
        }
        if (fullDocumentBeforeChange != null) {
            this._changeStream = this._changeStream.fullDocumentBeforeChange(fullDocumentBeforeChange);
        }
        //TODO: add handling when resume token is invalid
        //Case 1: out of oplog windows
        //Case 2: sometimes happened in AUTO_SCALE mode, seems like the partitioned change stream will have different series of resume token? (Not confirmed yet)
        //Command execution failed on MongoDB server with error 280 (ChangeStreamFatalError): 'PlanExecutor error during aggregation :: caused by :: cannot resume stream; the resume token was not found. {_data: "826A68AEB0000000012B042C0100296E5A10042E714C8BD34F4748B84E1EC13007D7AE463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F696400646A68AEB07ABA0D459AB8CD18000004"}' on server mzinx-cluster-shard-00-01.y8j6q.mongodb.net:27017. The full response is {"errorLabels": ["NonResumableChangeStreamError"], "ok": 0.0, "errmsg": "PlanExecutor error during aggregation :: caused by :: cannot resume stream; the resume token was not found. {_data: \"826A68AEB0000000012B042C0100296E5A10042E714C8BD34F4748B84E1EC13007D7AE463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F696400646A68AEB07ABA0D459AB8CD18000004\"}", "code": 280, "codeName": "ChangeStreamFatalError", "$clusterTime": {"clusterTime": {"$timestamp": {"t": 1785245360, "i": 6}}, "signature": {"hash": {"$binary": {"base64": "H5d6YuZdWQ4RwOwACgxUQHUaciM=", "subType": "00"}}, "keyId": 7606388459900502017}}, "operationTime": {"$timestamp": {"t": 1785245360, "i": 6}}}
        this.cursor = this._changeStream.cursor();
        ScheduledExecutorService scheduler = null;
        if (ResumeStrategy.TIME == this.getResumeStrategy()) {
            scheduler = this.timer(this, checkPoint);
        }

        try {
            while (this.isRunning()) {
                ChangeStreamDocument<T> e = this.getCursor().tryNext();
                if (e != null) {
                    this.getConsumer().execute(e);
                    if ((ResumeStrategy.BATCH == this.getResumeStrategy() && this.getCursor().available() == 0)
                            || ResumeStrategy.EVERY == this.getResumeStrategy()) {
                        checkPoint.accept(e.getResumeToken().getString("_data"));
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

    private List<Bson> getScaledPipeline(ChangeStreamRegistry<T> reg) {
        List<Bson> list = new ArrayList<>(this.pipeline);
        if (Mode.AUTO_SCALE == reg.getChangeStream().getMode() && reg.getInstanceSize() > 0
                && reg.getInstanceIndex() >= 0) {
            list.add(new Document("$match",
                    new Document("$expr",
                            new Document("$eq", Arrays.asList(new Document("$abs",
                                    new Document("$mod", Arrays.asList(
                                            new Document("$toHashedIndexKey", "$documentKey._id"),
                                            reg.getInstanceSize()))),
                                    reg.getInstanceIndex()))))
                    .toBsonDocument());
        }
        return list;
    }

    private ScheduledExecutorService timer(ChangeStream<T> cs, Consumer<BsonString> checkPoint) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    if (cs.isRunning()) {
                        if (cs.getCursor().getResumeToken() != null)
                            checkPoint.accept(cs.getCursor().getResumeToken().getString("_data"));
                    } else {
                        scheduler.shutdown();
                        logger.info("timer stopped");
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
                    cs.setRunning(false);
                    throw e;
                }
            }
        };
        logger.info("start timer:" + cs.getSaveTokenInterval());
        scheduler.scheduleAtFixedRate(task, 0, cs.getSaveTokenInterval(), TimeUnit.MILLISECONDS);
        return scheduler;
    }

    public static ChangeStream<Document> of(String id) {
        return of(id, Mode.BOARDCAST);
    }

    public static ChangeStream<Document> of(String id, Mode mode) {
        return of(id, mode, null);
    }

    public static ChangeStream<Document> of(String id, Mode mode,
            List<Bson> pipeline) {
        return new ChangeStream<Document>(id, mode, null, null, ResumeStrategy.NONE, DEFAULT_SAVE_TOKEN_INTERVAL, null,
                null,
                pipeline,
                Document.class);
    }

    public ChangeStream<T> batchSize(Integer batchSize) {
        return new ChangeStream<T>(this.id, this.mode, batchSize, this.maxAwaitTime,
                this.resumeStrategy, this.saveTokenInterval, this.fullDocumentBeforeChange, this.fullDocument,
                this.pipeline, this.documentClass);
    }

    public ChangeStream<T> maxAwaitTime(Long maxAwaitTime) {
        return new ChangeStream<T>(this.id, this.mode, this.batchSize, maxAwaitTime,
                this.resumeStrategy, this.saveTokenInterval, this.fullDocumentBeforeChange, this.fullDocument,
                this.pipeline, this.documentClass);
    }

    public ChangeStream<T> resumeStrategy(ResumeStrategy resumeStrategy) {
        return new ChangeStream<T>(this.id, this.mode, this.batchSize, maxAwaitTime,
                resumeStrategy, this.saveTokenInterval, this.fullDocumentBeforeChange, this.fullDocument, this.pipeline,
                this.documentClass);
    }

    public ChangeStream<T> resumeStrategy(ResumeStrategy resumeStrategy, long saveTokenInterval) {
        return new ChangeStream<T>(this.id, this.mode, this.batchSize, maxAwaitTime,
                resumeStrategy, saveTokenInterval, this.fullDocumentBeforeChange, this.fullDocument, this.pipeline,
                this.documentClass);
    }

    public ChangeStream<T> resumeAfter(String resumeToken) {
        this.resumeToken = resumeToken;
        return this;
    }

    public ChangeStream<T> fullDocumentBeforeChange(FullDocumentBeforeChange fullDocumentBeforeChange) {
        return new ChangeStream<T>(this.id, this.mode, this.batchSize, maxAwaitTime,
                this.resumeStrategy, this.saveTokenInterval, fullDocumentBeforeChange, this.fullDocument, this.pipeline,
                this.documentClass);
    }

    public ChangeStream<T> fullDocument(FullDocument fullDocument) {
        return new ChangeStream<T>(this.id, this.mode, this.batchSize, maxAwaitTime,
                this.resumeStrategy, this.saveTokenInterval, this.fullDocumentBeforeChange, fullDocument, this.pipeline,
                this.documentClass);
    }

    public <NewT> ChangeStream<NewT> withClass(Class<NewT> clazz) {
        return new ChangeStream<NewT>(this.id, this.mode, this.batchSize, this.maxAwaitTime,
                this.resumeStrategy, this.saveTokenInterval, this.fullDocumentBeforeChange, this.fullDocument,
                this.pipeline, clazz);
    }

}
