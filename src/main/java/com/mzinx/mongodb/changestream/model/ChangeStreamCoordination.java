package com.mzinx.mongodb.changestream.model;

import java.util.Date;
import java.util.List;

import org.bson.Document;

import lombok.Builder;
import lombok.Data;

/**
 * Immutable snapshot of a change stream coordination document stored in the
 * {@code change-stream.changeStreamCollection} collection. This document is
 * the single source of truth for the distributed state of a change stream:
 *
 * <pre>
 * {
 *   _id: "csId",
 *   l:   { h: "host-a", until: ISODate },  // leader lease (null = no leader)
 *   t:   NumberLong(7),                    // fencing term, bumped on every leadership change
 *   i:   ["host-a", "host-b"],             // sorted member hostnames
 *   e:   NumberLong(12),                   // membership epoch, bumped on every membership change
 *   at:  ISODate                           // server time of the last effective change
 * }
 * </pre>
 *
 * All timestamps are MongoDB server time ({@code $$NOW}), so lease expiry
 * never depends on application clocks. Legacy documents that store the leader
 * as a plain string ({@code l: "host"}) are parsed as leaderless with an
 * expired lease and get migrated lazily by the first atomic update touching
 * them.
 */
@Data
@Builder
public class ChangeStreamCoordination {

    public static final String LEADER_FIELD = "l";
    public static final String LEADER_HOST_FIELD = "h";
    public static final String LEADER_UNTIL_FIELD = "until";
    public static final String TERM_FIELD = "t";
    public static final String MEMBERS_FIELD = "i";
    public static final String EPOCH_FIELD = "e";
    public static final String DATE_FIELD = "at";

    /** Change stream id ({@code _id} of the coordination document). */
    private String id;

    /** Hostname currently holding the leader lease, or {@code null}. */
    private String leader;

    /** Server-side expiry of the current leader lease, or {@code null}. */
    private Date leaseUntil;

    /** Monotonic fencing term; bumped whenever leadership changes holder. */
    private long term;

    /** Sorted hostnames registered for this change stream. */
    private List<String> members;

    /** Monotonic membership epoch; bumped whenever {@link #members} changes. */
    private long epoch;

    /** Server time of the last effective coordination change. */
    private Date at;

    public boolean isMember(String host) {
        return this.members != null && this.members.contains(host);
    }

    public boolean isLeader(String host) {
        return host != null && host.equals(this.leader);
    }

    /**
     * Parses a coordination document, tolerating legacy documents where
     * {@code l} is a plain hostname string and {@code t}/{@code e} are absent.
     */
    public static ChangeStreamCoordination from(Document doc) {
        if (doc == null)
            return null;
        String leader = null;
        Date leaseUntil = null;
        Object l = doc.get(LEADER_FIELD);
        if (l instanceof Document lease) {
            leader = lease.getString(LEADER_HOST_FIELD);
            leaseUntil = lease.getDate(LEADER_UNTIL_FIELD);
        }
        // legacy string leader: treated as leaderless (expired lease), the next
        // atomic update migrates the document to the lease shape
        List<String> members = doc.getList(MEMBERS_FIELD, String.class);
        return ChangeStreamCoordination.builder()
                .id(doc.getString("_id"))
                .leader(leader)
                .leaseUntil(leaseUntil)
                .term(asLong(doc.get(TERM_FIELD)))
                .members(members == null ? List.of() : List.copyOf(members))
                .epoch(asLong(doc.get(EPOCH_FIELD)))
                .at(doc.getDate(DATE_FIELD))
                .build();
    }

    private static long asLong(Object value) {
        return value instanceof Number n ? n.longValue() : 0L;
    }
}
