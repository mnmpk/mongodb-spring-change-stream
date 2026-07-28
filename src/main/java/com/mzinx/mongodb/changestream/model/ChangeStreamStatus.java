package com.mzinx.mongodb.changestream.model;

import java.util.List;

import com.mzinx.mongodb.changestream.model.ChangeStream.Mode;
import com.mzinx.mongodb.changestream.model.ChangeStream.ResumeStrategy;

import lombok.Builder;
import lombok.Data;

/**
 * Read-only snapshot of a registered change stream and its runtime status,
 * exposed by
 * {@link com.mzinx.mongodb.changestream.bootstrap.ChangeStreamManager}.
 */
@Data
@Builder
public class ChangeStreamStatus {

    /** Unique change stream id. */
    private String id;

    /** Watched collection, or {@code null} when the whole database is watched. */
    private String collectionName;

    /** Coordination mode of the change stream. */
    private Mode mode;

    /** Resume token checkpoint strategy. */
    private ResumeStrategy resumeStrategy;

    /** Whether the change stream cursor is currently running on this instance. */
    private boolean running;

    /** Hostname of the current leader instance. */
    private String leader;

    /** Hostnames of all instances registered for this change stream. */
    private List<String> instances;

    /** Index of this instance among the cluster instances (AUTO_SCALE). */
    private int instanceIndex;

    /** Number of cluster instances sharing the stream (AUTO_SCALE). */
    private int instanceSize;

    /** Last resume token applied to the stream, if any. */
    private String resumeToken;

    /** Simple class name of the listener handling the events. */
    private String listener;

    /**
     * Whether this change stream is driven by a persisted
     * {@link ChangeStreamConfig} (as opposed to internal streams such as the
     * coordination stream, or streams started programmatically).
     */
    private boolean managedByConfig;
}
