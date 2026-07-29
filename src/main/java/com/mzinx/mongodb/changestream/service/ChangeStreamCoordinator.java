package com.mzinx.mongodb.changestream.service;

import static com.mzinx.mongodb.changestream.model.ChangeStreamCoordination.DATE_FIELD;
import static com.mzinx.mongodb.changestream.model.ChangeStreamCoordination.EPOCH_FIELD;
import static com.mzinx.mongodb.changestream.model.ChangeStreamCoordination.LEADER_FIELD;
import static com.mzinx.mongodb.changestream.model.ChangeStreamCoordination.LEADER_HOST_FIELD;
import static com.mzinx.mongodb.changestream.model.ChangeStreamCoordination.LEADER_UNTIL_FIELD;
import static com.mzinx.mongodb.changestream.model.ChangeStreamCoordination.MEMBERS_FIELD;
import static com.mzinx.mongodb.changestream.model.ChangeStreamCoordination.TERM_FIELD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReturnDocument;
import com.mzinx.mongodb.changestream.config.ChangeStreamProperties;
import com.mzinx.mongodb.changestream.model.ChangeStreamCoordination;

/**
 * Encapsulates every operation on the change stream coordination collection
 * ({@code change-stream.changeStreamCollection}) and the instance liveness
 * collection ({@code change-stream.instanceCollection}).
 * <p>
 * The coordination document is the single source of truth for the distributed
 * state of a change stream (see {@link ChangeStreamCoordination}). All
 * mutations are single atomic {@code findOneAndUpdate}/{@code updateMany}
 * aggregation-pipeline updates evaluated with MongoDB server time
 * ({@code $$NOW}), so concurrent instances can never corrupt the state and
 * lease expiry never depends on application clocks. Updates that would not
 * change the document are no-ops on the server and therefore do not emit
 * change stream events.
 */
@Service
public class ChangeStreamCoordinator {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final Date UNIX_EPOCH = new Date(0);

    private final MongoTemplate mongoTemplate;
    private final ChangeStreamProperties properties;

    ChangeStreamCoordinator(MongoTemplate mongoTemplate, ChangeStreamProperties changeStreamProperties) {
        this.mongoTemplate = mongoTemplate;
        this.properties = changeStreamProperties;
    }

    private MongoCollection<Document> coordinationCollection() {
        return mongoTemplate.getCollection(properties.getCoordinationCollection());
    }

    private MongoCollection<Document> instanceCollection() {
        return mongoTemplate.getCollection(properties.getInstanceCollection());
    }

    private String hostname() {
        return properties.getHostname();
    }

    /**
     * Registers this host as a member of the change stream, keeping the member
     * list sorted and bumping the membership epoch only when membership
     * actually changes. Also lazily migrates legacy documents (string leader)
     * to the lease shape. Returns the resulting coordination state, which
     * doubles as the database-to-memory synchronization read.
     */
    public ChangeStreamCoordination join(String streamId) {
        Document alreadyMember = expr("$in", hostname(), "$_m");
        List<Bson> pipeline = List.of(
                new Document("$set", new Document()
                        .append("_m", ifNull("$" + MEMBERS_FIELD, List.of()))
                        .append("_l", normalizedLease())),
                new Document("$set", new Document()
                        .append(MEMBERS_FIELD, new Document("$sortArray", new Document()
                                .append("input", expr("$setUnion", "$_m", List.of(hostname())))
                                .append("sortBy", 1)))
                        .append(LEADER_FIELD, "$_l")
                        .append(TERM_FIELD, ifNull("$" + TERM_FIELD, 0L))),
                new Document("$set", new Document()
                        .append(EPOCH_FIELD, cond(alreadyMember,
                                ifNull("$" + EPOCH_FIELD, 0L),
                                expr("$add", ifNull("$" + EPOCH_FIELD, 0L), 1L)))
                        .append(DATE_FIELD, cond(alreadyMember, ifNull("$" + DATE_FIELD, "$$NOW"), "$$NOW"))),
                new Document("$unset", List.of("_m", "_l")));
        return ChangeStreamCoordination.from(coordinationCollection().findOneAndUpdate(
                Filters.eq("_id", streamId), pipeline,
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)));
    }

    /**
     * Atomically acquires the leader lease if it is free or expired, or renews
     * it if this host already holds it. The fencing term is bumped only when
     * leadership changes holder, never on renewal, so followers can ignore
     * pure renewal events and checkpoints can be fenced against zombie
     * leaders. Lease expiry is evaluated against {@code $$NOW} (server time).
     */
    public ChangeStreamCoordination acquireOrRenewLease(String streamId) {
        List<Bson> pipeline = List.of(
                new Document("$set", new Document("_l", normalizedLease())),
                new Document("$set", new Document()
                        .append("_mine", expr("$eq", ifNull("$_l." + LEADER_HOST_FIELD, null), hostname()))
                        .append("_open", expr("$or",
                                expr("$eq", ifNull("$_l", null), null),
                                expr("$lt", ifNull("$_l." + LEADER_UNTIL_FIELD, UNIX_EPOCH), "$$NOW")))),
                new Document("$set", new Document()
                        .append("_acq", expr("$or", "$_mine", "$_open"))
                        .append("_bump", expr("$and", expr("$not", "$_mine"), "$_open"))),
                new Document("$set", new Document()
                        .append(LEADER_FIELD, cond("$_acq",
                                new Document(LEADER_HOST_FIELD, hostname())
                                        .append(LEADER_UNTIL_FIELD,
                                                expr("$add", "$$NOW", properties.getLeaseDuration())),
                                "$_l"))
                        .append(TERM_FIELD, cond("$_bump",
                                expr("$add", ifNull("$" + TERM_FIELD, 0L), 1L),
                                ifNull("$" + TERM_FIELD, 0L)))
                        .append(DATE_FIELD, cond("$_bump", "$$NOW", ifNull("$" + DATE_FIELD, "$$NOW")))),
                new Document("$unset", List.of("_l", "_mine", "_open", "_acq", "_bump")));
        return ChangeStreamCoordination.from(coordinationCollection().findOneAndUpdate(
                Filters.eq("_id", streamId), pipeline,
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)));
    }

    /**
     * Deregisters this host from the change stream: removes it from the member
     * list (bumping the epoch) and releases the leader lease if this host
     * holds it. Returns the resulting state, or {@code null} when no
     * coordination document exists.
     */
    public ChangeStreamCoordination leave(String streamId) {
        Document wasLeader = expr("$eq", ifNull("$_l." + LEADER_HOST_FIELD, null), hostname());
        Document membershipChanged = expr("$ne",
                new Document("$size", "$" + MEMBERS_FIELD), new Document("$size", "$_m"));
        List<Bson> pipeline = List.of(
                new Document("$set", new Document()
                        .append("_m", ifNull("$" + MEMBERS_FIELD, List.of()))
                        .append("_l", normalizedLease())),
                new Document("$set", new Document()
                        .append(MEMBERS_FIELD, new Document("$filter", new Document()
                                .append("input", "$_m")
                                .append("as", "m")
                                .append("cond", expr("$ne", "$$m", hostname()))))
                        .append(LEADER_FIELD, cond(wasLeader, null, "$_l"))),
                new Document("$set", new Document()
                        .append(EPOCH_FIELD, cond(membershipChanged,
                                expr("$add", ifNull("$" + EPOCH_FIELD, 0L), 1L),
                                ifNull("$" + EPOCH_FIELD, 0L)))
                        .append(TERM_FIELD, ifNull("$" + TERM_FIELD, 0L))
                        .append(DATE_FIELD, cond(expr("$or", membershipChanged, wasLeader),
                                "$$NOW", ifNull("$" + DATE_FIELD, "$$NOW")))),
                new Document("$unset", List.of("_m", "_l")));
        return ChangeStreamCoordination.from(coordinationCollection().findOneAndUpdate(
                Filters.eq("_id", streamId), pipeline,
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
    }

    /**
     * Clears the leader lease and the whole member list of the change stream,
     * bumping both term and epoch so every instance fences and re-evaluates.
     */
    public ChangeStreamCoordination reset(String streamId) {
        List<Bson> pipeline = List.of(
                new Document("$set", new Document()
                        .append(LEADER_FIELD, null)
                        .append(MEMBERS_FIELD, List.of())
                        .append(TERM_FIELD, expr("$add", ifNull("$" + TERM_FIELD, 0L), 1L))
                        .append(EPOCH_FIELD, expr("$add", ifNull("$" + EPOCH_FIELD, 0L), 1L))
                        .append(DATE_FIELD, "$$NOW")));
        return ChangeStreamCoordination.from(coordinationCollection().findOneAndUpdate(
                Filters.eq("_id", streamId), pipeline,
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)));
    }

    /**
     * Membership repair across <b>all</b> coordination documents: removes
     * members that are not in the given alive set and releases leases held by
     * dead hosts or already expired. Documents that need no repair are left
     * untouched (no write, no change event), so this is safe and quiet to run
     * from every instance on every reconcile cycle.
     */
    public void repair(Collection<String> aliveHosts) {
        List<String> alive = List.copyOf(aliveHosts);
        Document membershipChanged = expr("$ne",
                new Document("$size", "$" + MEMBERS_FIELD), new Document("$size", "$_m"));
        Document leaseReleased = expr("$and",
                expr("$ne", ifNull("$_l", null), null),
                expr("$eq", ifNull("$" + LEADER_FIELD, null), null));
        List<Bson> pipeline = List.of(
                new Document("$set", new Document()
                        .append("_m", ifNull("$" + MEMBERS_FIELD, List.of()))
                        .append("_l", normalizedLease())),
                new Document("$set", new Document()
                        .append(MEMBERS_FIELD, new Document("$filter", new Document()
                                .append("input", "$_m")
                                .append("as", "m")
                                .append("cond", expr("$in", "$$m", alive))))
                        .append(LEADER_FIELD, cond(
                                expr("$or",
                                        expr("$eq", ifNull("$_l", null), null),
                                        expr("$not", expr("$in", ifNull("$_l." + LEADER_HOST_FIELD, null), alive)),
                                        expr("$lt", ifNull("$_l." + LEADER_UNTIL_FIELD, UNIX_EPOCH), "$$NOW")),
                                null,
                                "$_l"))),
                new Document("$set", new Document()
                        .append(EPOCH_FIELD, cond(membershipChanged,
                                expr("$add", ifNull("$" + EPOCH_FIELD, 0L), 1L),
                                ifNull("$" + EPOCH_FIELD, 0L)))
                        .append(TERM_FIELD, ifNull("$" + TERM_FIELD, 0L))
                        .append(DATE_FIELD, cond(expr("$or", membershipChanged, leaseReleased),
                                "$$NOW", ifNull("$" + DATE_FIELD, "$$NOW")))),
                new Document("$unset", List.of("_m", "_l")));
        coordinationCollection().updateMany(Filters.empty(), pipeline);
    }

    /** Loads all coordination documents. */
    public List<ChangeStreamCoordination> findAll() {
        List<ChangeStreamCoordination> result = new ArrayList<>();
        coordinationCollection().find().forEach(doc -> result.add(ChangeStreamCoordination.from(doc)));
        return result;
    }

    /** Loads the coordination document of a change stream, or {@code null}. */
    public ChangeStreamCoordination find(String streamId) {
        return ChangeStreamCoordination.from(coordinationCollection().find(Filters.eq("_id", streamId)).first());
    }

    /**
     * Deletes coordination documents that are not in the given id set and have
     * neither members nor a leader left (nobody runs or wants them anymore).
     * The empty-member filter makes the cleanup safe against races with
     * {@link #join(String)}, which always registers the joining host in the
     * same atomic update.
     */
    public long deleteOrphans(Collection<String> keepIds) {
        return coordinationCollection().deleteMany(Filters.and(
                Filters.nin("_id", keepIds),
                Filters.eq(MEMBERS_FIELD, List.of()),
                Filters.eq(LEADER_FIELD, null))).getDeletedCount();
    }

    /**
     * Actively deletes instance documents whose heartbeat is older than
     * {@code change-stream.maxTimeout}, instead of waiting for the TTL monitor
     * (which only sweeps about once a minute). This shortens failover latency
     * and produces the delete events the fast reconcile path reacts to.
     */
    public long sweepDeadInstances() {
        Date cutoff = new Date(System.currentTimeMillis() - properties.getInstanceLivenessTimeout());
        return instanceCollection().deleteMany(Filters.lt(DATE_FIELD, cutoff)).getDeletedCount();
    }

    /**
     * Returns the hostnames with a fresh heartbeat, always including this
     * host (so a locally lagging heartbeat can never evict ourselves). Returns
     * an empty list when the instance collection is completely empty, meaning
     * no discovery/heartbeat mechanism is active and liveness is unknown.
     */
    public List<String> aliveInstances() {
        if (instanceCollection().countDocuments() == 0)
            return List.of();
        Date cutoff = new Date(System.currentTimeMillis() - properties.getInstanceLivenessTimeout());
        List<String> alive = new ArrayList<>();
        instanceCollection().find(Filters.gte(DATE_FIELD, cutoff))
                .projection(Projections.include("_id"))
                .map(d -> d.getString("_id"))
                .into(alive);
        if (!alive.contains(hostname()))
            alive.add(hostname());
        return alive;
    }

    /**
     * Normalizes the leader field to the lease shape: legacy string leaders
     * and missing values become {@code null} (treated as an open lease).
     */
    private static Document normalizedLease() {
        return cond(expr("$eq", new Document("$type", "$" + LEADER_FIELD), "string"),
                null,
                ifNull("$" + LEADER_FIELD, null));
    }

    /** Null-tolerant {@code $cond} expression builder. */
    private static Document cond(Object condition, Object then, Object otherwise) {
        return new Document("$cond", Arrays.asList(condition, then, otherwise));
    }

    /** Null-tolerant aggregation operator expression builder. */
    private static Document expr(String operator, Object... args) {
        return new Document(operator, Arrays.asList(args));
    }

    private static Document ifNull(Object expression, Object fallback) {
        return expr("$ifNull", expression, fallback);
    }
}
