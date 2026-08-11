package com.mzinx.mongodb.changestream.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
 * The {@link com.mzinx.mongodb.changestream.ChangeStreamManager}
 * periodically fetches these configs and manages the corresponding change
 * stream life cycles (start, restart on change, stop on removal/disable).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChangeStreamConfig {

    private static final Long DEFAULT_CHECKPOINT_INTERVAL = 60 * 1000l;

    /** Unique change stream id. */
    @Id
    private String id;

    /** Collection to watch. When {@code null}, the whole database is watched. */
    private String collectionName;

    /** Coordination mode. Defaults to {@link Mode#BROADCAST}. */
    @Builder.Default
    private Mode mode = Mode.BROADCAST;

    private Integer batchSize;

    /** Max await time in milliseconds. */
    private Long maxAwaitTime;

    /** Resume token checkpoint strategy. Defaults to {@link ResumeStrategy#NONE}. */
    @Builder.Default
    private ResumeStrategy resumeStrategy = ResumeStrategy.NONE;

    /** Interval in milliseconds for {@link ResumeStrategy#INTERVAL} checkpointing. */
    private Long checkpointInterval;

    private FullDocument fullDocument;

    private FullDocumentBeforeChange fullDocumentBeforeChange;

    /** Aggregation pipeline stages applied to the change stream. */
    private List<Document> pipeline;

    /**
     * Name of the {@link com.mzinx.mongodb.changestream.listener.ChangeStreamListener}
     * Spring bean handling the events of this change stream.
     */
    private String listener;

    /**
     * Which app runs this stream when a business app and a management app
     * (mongostream) share one config collection ({@code _changeStreamConfigs}).
     * An app runs a config only when {@link RunOn} matches its role
     * ({@code change-stream.manager}) <em>and</em> its {@link #listener} bean is
     * present locally. Non-matching configs stay visible to both apps (so the
     * management console can list/create/start/stop/edit them) but run on exactly
     * one side. Defaults to {@link RunOn#BUSINESS}.
     */
    @Builder.Default
    private RunOn runOn = RunOn.BUSINESS;

    /**
     * Where a change stream config is allowed to run, relative to the app's role
     * ({@code change-stream.manager}).
     */
    public enum RunOn {
        /** Runs on the business app (the non-manager). The default for app streams. */
        BUSINESS,
        /** Runs on the management app (mongostream, {@code change-stream.manager=true}). */
        MANAGER,
        /**
         * Runs wherever the listener bean is present, regardless of role — used
         * by the libraries' own internal streams (e.g. discovery, which both apps
         * run).
         */
        ANY
    }

    /** Whether this change stream should be running. */
    @Builder.Default
    private boolean enabled = true;

    /**
     * Free-form, listener-defined attributes for this change stream. The library
     * does not interpret these; a listener bean can stash custom configuration
     * here (for example, the name of an output aggregation pipeline to run) and
     * read it back from its config at event time. Persisted as-is.
     */
    private Map<String, Object> attributes;

    /** Last modification time, maintained on save. */
    private Date updatedAt;

    /**
     * Builds the runtime {@link ChangeStream} represented by this config.
     */
    public ChangeStream<Document> toChangeStream() {
        List<Bson> stages = this.pipeline == null ? List.of() : new ArrayList<>(this.pipeline);
        ChangeStream<Document> stream = new ChangeStream<>(this.id,
                this.mode == null ? Mode.BROADCAST : this.mode,
                this.batchSize,
                this.maxAwaitTime,
                this.resumeStrategy == null ? ResumeStrategy.NONE : this.resumeStrategy,
                this.checkpointInterval == null ? DEFAULT_CHECKPOINT_INTERVAL : this.checkpointInterval,
                this.fullDocumentBeforeChange,
                this.fullDocument,
                stages,
                Document.class);
        // Carry the config's attributes on the runtime stream so the listener
        // receives them on every event without a per-event config lookup.
        stream.setAttributes(this.attributes);
        return stream;
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
                && Objects.equals(this.checkpointInterval, other.checkpointInterval)
                && this.fullDocument == other.fullDocument
                && this.fullDocumentBeforeChange == other.fullDocumentBeforeChange
                && Objects.equals(this.pipeline, other.pipeline)
                && Objects.equals(this.listener, other.listener)
                && Objects.equals(this.attributes, other.attributes)
                && this.runOn == other.runOn
                && this.enabled == other.enabled;
    }
}
