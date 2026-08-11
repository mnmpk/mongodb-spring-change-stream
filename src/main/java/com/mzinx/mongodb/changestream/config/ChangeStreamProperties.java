package com.mzinx.mongodb.changestream.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Data
@ConfigurationProperties("change-stream")
@Component
public class ChangeStreamProperties {
    private boolean enabled = true;
    private String hostname = System.getenv().getOrDefault("HOSTNAME", "localhost");
    /**
     * This app's role when a business app and a management console (mongostream)
     * share one config collection. Set {@code true} on the management console so
     * it runs configs marked {@code runOn=MANAGER} (and skips {@code BUSINESS}
     * ones); leave {@code false} (default) on the business app, which runs
     * {@code runOn=BUSINESS} configs. Streams marked {@code runOn=ANY} run on
     * either, gated only by listener-bean presence.
     */
    private boolean manager = false;
    private long batchSize = 1000;
    private long maxAwaitTime = 800; // ms
    private long tokenMaxLifeTime = 86400000; // ms
    /**
     * Instance liveness timeout in ms: hosts whose heartbeat in the instance
     * collection is older than this are considered dead and are swept and
     * repaired out of every coordination document. Should be aligned with
     * {@code discovery.heartbeat.interval * discovery.heartbeat.max}.
     */
    private long instanceLivenessTimeout = 5000 * 10; // ms
    /**
     * Leader lease duration in ms (AUTO_RECOVER mode). The lease is renewed on
     * every reconcile cycle, so it should be a small multiple of
     * {@code configRefreshInterval} to survive transient pauses without
     * causing spurious failovers.
     */
    private long leaseDuration = 90000; // ms
    private String resumeTokenCollection = "_resumeTokens";
    private String instanceCollection = "_instances";
    /** Collection holding the change stream coordination documents. */
    private String coordinationCollection = "_changeStreams";
    private String changeStreamConfigCollection = "_changeStreamConfigs";
    private long configRefreshInitialDelay = 10000; // ms
    private long configRefreshInterval = 30000; // ms
}
