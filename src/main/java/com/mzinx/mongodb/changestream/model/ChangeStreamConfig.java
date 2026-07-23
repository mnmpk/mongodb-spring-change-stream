package com.mzinx.mongodb.changestream.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.annotation.Id;

import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mzinx.mongodb.changestream.model.ChangeStream.Mode;
import com.mzinx.mongodb.changestream.model.ChangeStream.ResumeStrategy;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Persistent definition of a change stream, stored in the
 * {@code change-stream.changeStreamConfigCollection} collection.
 * <p>
 * The {@link com.mzinx.mongodb.changestream.bootstrap.ChangeStreamManager}
 * periodically fetches these configs and manages the corresponding change
 * stream life cycles (start, restart on change, stop on removal/disable).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChangeStreamConfig {

    private static final Long DEFAULT_SAVE_TOKEN_INTERVAL = 60 * 1000l;

    /** Unique change stream id. */
    @Id
    private String id;

    /** Collection to watch. When {@code null}, the whole database is watched. */
    private String collectionName;

    /** Coordination mode. Defaults to {@link Mode#BOARDCAST}. */
    @Builder.Default
    private Mode mode = Mode.BOARDCAST;

    private Integer batchSize;

    /** Max await time in milliseconds. */
    private Long maxAwaitTime;

    /** Resume token checkpoint strategy. Defaults to {@link ResumeStrategy#NONE}. */
    @Builder.Default
    private ResumeStrategy resumeStrategy = ResumeStrategy.NONE;

    /** Interval in milliseconds for {@link ResumeStrategy#TIME} checkpointing. */
    private Long saveTokenInterval;

    private FullDocument fullDocument;

    private FullDocumentBeforeChange fullDocumentBeforeChange;

    /** Aggregation pipeline stages applied to the change stream. */
    private List<Document> pipeline;

    /**
     * Name of the {@link com.mzinx.mongodb.changestream.listener.ChangeStreamListener}
     * Spring bean handling the events of this change stream.
     */
    private String listener;

    /** Whether this change stream should be running. */
    @Builder.Default
    private boolean enabled = true;

    /** Last modification time, maintained on save. */
    private Date updatedAt;

    /**
     * Builds the runtime {@link ChangeStream} represented by this config.
     */
    public ChangeStream<Document> toChangeStream() {
        List<Bson> stages = this.pipeline == null ? List.of() : new ArrayList<>(this.pipeline);
        return new ChangeStream<>(this.id,
                this.mode == null ? Mode.BOARDCAST : this.mode,
                this.batchSize,
                this.maxAwaitTime,
                this.resumeStrategy == null ? ResumeStrategy.NONE : this.resumeStrategy,
                this.saveTokenInterval == null ? DEFAULT_SAVE_TOKEN_INTERVAL : this.saveTokenInterval,
                this.fullDocumentBeforeChange,
                this.fullDocument,
                stages,
                Document.class);
    }

    /**
     * Compares the effective definition of two configs, ignoring
     * {@code updatedAt}, to decide whether a running stream must be restarted.
     */
    public boolean isSameDefinition(ChangeStreamConfig other) {
        if (other == null)
            return false;
        return Objects.equals(this.id, other.id)
                && Objects.equals(this.collectionName, other.collectionName)
                && this.mode == other.mode
                && Objects.equals(this.batchSize, other.batchSize)
                && Objects.equals(this.maxAwaitTime, other.maxAwaitTime)
                && this.resumeStrategy == other.resumeStrategy
                && Objects.equals(this.saveTokenInterval, other.saveTokenInterval)
                && this.fullDocument == other.fullDocument
                && this.fullDocumentBeforeChange == other.fullDocumentBeforeChange
                && Objects.equals(this.pipeline, other.pipeline)
                && Objects.equals(this.listener, other.listener)
                && this.enabled == other.enabled;
    }
}
