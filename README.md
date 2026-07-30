# MongoDB Spring Change Stream

A Spring Boot starter library that provides advanced MongoDB change stream capabilities with auto-recovery, auto-scaling, resume token management, and distributed processing support.

## Features

- **Multiple Operation Modes**: BROADCAST, AUTO_RECOVER, and AUTO_SCALE modes for different deployment scenarios
- **Resume Token Management**: Automatic saving and resuming of change stream positions to prevent data loss
- **Auto-Recovery**: Automatically restart change streams on node failures in distributed environments
- **Auto-Scaling**: Distribute change stream processing across multiple instances for load balancing
- **Pipeline Support**: Apply aggregation pipelines to filter and transform change stream events
- **Batch Processing**: Configurable batch sizes and await times for optimized performance
- **TTL-based Cleanup**: Automatic cleanup of old resume tokens using MongoDB TTL indexes
- **Spring Integration**: Seamless integration with Spring Boot and MongoDB

## Installation

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.mzinx</groupId>
    <artifactId>mongodb-spring-change-stream</artifactId>
    <version>1.0.0</version>
</dependency>
```

Also add the aggregation dependency for pipeline support:

```xml
<dependency>
    <groupId>com.mzinx</groupId>
    <artifactId>mongodb-spring-aggregation</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Configuration

Configure the library using the following properties in your `application.properties` or `application.yml`:

```properties
# Enable/disable change stream functionality (default: true)
change-stream.enabled=true

# Hostname for instance identification (default: system HOSTNAME or localhost)
change-stream.hostname=localhost

# Batch size for change stream processing (default: 1000)
change-stream.batchSize=1000

# Maximum await time in milliseconds (default: 800)
change-stream.maxAwaitTime=800

# Maximum lifetime for resume tokens in milliseconds (default: 86400000 = 24 hours)
change-stream.tokenMaxLifeTime=86400000

# Instance liveness timeout in milliseconds (default: 50000). Hosts whose
# heartbeat in the instance collection is older than this are swept and
# repaired out of every coordination document. Align it with
# discovery.heartbeat.interval * discovery.heartbeat.max.
change-stream.instanceLivenessTimeout=50000

# Leader lease duration in milliseconds for AUTO_RECOVER mode (default: 90000).
# The lease is renewed on every reconcile cycle, so keep it a small multiple of
# change-stream.configRefreshInterval.
change-stream.leaseDuration=90000

# Collection name for storing resume tokens (default: _resumeTokens)
change-stream.resumeTokenCollection=_resumeTokens

# Collection name for storing instance information (default: _instances)
change-stream.instanceCollection=_instances

# Collection name for change stream coordination documents (default: _changeStreams)
change-stream.coordinationCollection=_changeStreams

# Collection name for storing change stream configs (default: _changeStreamConfigs)
change-stream.changeStreamConfigCollection=_changeStreamConfigs

# Initial delay before the first config fetch in milliseconds (default: 10000)
change-stream.configRefreshInitialDelay=10000

# Interval between config fetches in milliseconds (default: 30000)
change-stream.configRefreshInterval=30000
```

## Usage

### Config-Driven Change Streams

Change stream definitions can be stored in a MongoDB collection (default: `_changeStreamConfigs`). The `ChangeStreamManager` periodically fetches these configs and manages the change stream life cycles:

- starts change streams for new enabled configs
- restarts change streams whose definition changed
- stops change streams whose config was removed or disabled

Save a config with `ChangeStreamConfigService`:

```java
@Autowired
private ChangeStreamConfigService changeStreamConfigService;

changeStreamConfigService.save(ChangeStreamConfig.builder()
    .id("orders-stream")                 // unique change stream id
    .collectionName("orders")            // collection to watch (null = whole database)
    .mode(Mode.BROADCAST)                // BROADCAST, AUTO_RECOVER or AUTO_SCALE
    .resumeStrategy(ResumeStrategy.PER_BATCH)
    .pipeline(List.of(new Document("$match",
        new Document("operationType", new Document("$in", List.of("insert", "update"))))))
    .listener("orderListener")           // ChangeStreamListener bean name
    .enabled(true)
    .build());
```

The `listener` field references a Spring bean implementing `ChangeStreamListener<Document>`:

```java
@Component("orderListener")
public class OrderListener implements ChangeStreamListener<Document> {
    @Override
    public void onEvent(ChangeStreamDocument<Document> event) {
        System.out.println("Change detected: " + event.getOperationType());
    }
}
```

Disable a stream by saving the config with `enabled(false)`, or stop it permanently with `changeStreamConfigService.delete("orders-stream")`. Changes are picked up on the next refresh (`change-stream.configRefreshInterval`, default 30s).

### Programmatic Change Streams

Streams can also be registered programmatically through
`ChangeStreamService.start(...)`; they are reconciled and supervised exactly
like config-driven ones:

```java
@Autowired
private ChangeStreamService<Document> changeStreamService;

ChangeStreamRuntime<Document> runtime = ChangeStreamRuntime.<Document>builder()
    .collectionName("orders")            // null = whole database
    .listener(event -> System.out.println("Change detected: " + event.getOperationType()))
    .changeStream(ChangeStream.of("orders-stream", Mode.AUTO_RECOVER)
        .resumeStrategy(ResumeStrategy.INTERVAL, 30000)
        .fullDocument(FullDocument.UPDATE_LOOKUP))
    .build();

changeStreamService.start(runtime);

// stop locally and deregister this host from the coordination document
changeStreamService.stop(runtime);
// or stop the stream on every instance
changeStreamService.stopAllInstances(runtime);
```

### Monitoring Change Streams

`ChangeStreamManager` exposes a read-only status API covering every registered
stream (the coordination stream, config-driven and programmatic streams):

```java
@Autowired
private ChangeStreamManager changeStreamManager;

// all registered change streams
List<ChangeStreamStatus> all = changeStreamManager.getChangeStreams();

// streams currently running on this instance
List<ChangeStreamStatus> active = changeStreamManager.getActiveChangeStreams();

// a specific stream: running flag, leader, lease expiry, fencing term,
// members, membership epoch, partition index/count, resume token, listener
Optional<ChangeStreamStatus> status = changeStreamManager.getChangeStreamStatus("orders-stream");
```

## Operation Modes

### BROADCAST Mode
- Every registered member instance runs the full change stream
- No leader is needed or elected
- Suitable when all instances need all events (e.g. local cache invalidation)

### AUTO_RECOVER Mode
- Exactly one instance runs the stream: the holder of the leader lease
- Leadership is a server-time lease (`change-stream.leaseDuration`) stored in
  the coordination document; the holder renews it on every reconcile cycle and
  any other instance takes over atomically once the lease expires or the
  holder's heartbeat dies
- Every leadership change bumps a monotonic fencing term; resume token
  checkpoints are stamped with the term and resume selection prefers the
  highest term, so a deposed (zombie) leader can never move the legitimate
  resume position

### AUTO_SCALE Mode
- Every member runs a disjoint hash partition of the stream (document key
  hashing)
- Partitions are derived from the sorted member list of the coordination
  document, guarded by a membership epoch: all instances compute identical,
  non-overlapping partitions and repartition exactly once per membership
  change

## Coordination and Reconciliation

> A full design document — including component/sequence diagrams and every
> coordination aggregation pipeline in JSON — is available in
> [COORDINATION.md](COORDINATION.md).

The distributed state of every change stream lives in a single coordination
document (collection `change-stream.coordinationCollection`, default
`_changeStreams`) — the single source of truth:

```javascript
{
  _id: "csId",
  l:   { h: "host-a", until: ISODate },  // leader lease (null = no leader)
  t:   NumberLong(7),                    // fencing term, bumped on leadership change
  i:   ["host-a", "host-b"],             // sorted member hostnames
  e:   NumberLong(12),                   // membership epoch, bumped on membership change
  at:  ISODate                           // server time of the last effective change
}
```

All mutations are single atomic aggregation-pipeline updates evaluated with
MongoDB server time (`$$NOW`), so concurrent instances cannot corrupt the
state and lease expiry never depends on application clocks. Legacy documents
(pre-lease shape with `l` as a plain hostname string) are migrated lazily by
the first update touching them.

Every instance runs a periodic reconcile loop
(`change-stream.configRefreshInterval`) that:

1. sweeps dead instance heartbeats and atomically repairs every coordination
   document (removes dead members, releases dead/expired leases)
2. reconciles the persisted change stream configs (start/restart/stop)
3. synchronizes each local runtime from its coordination document and
   starts, stops or repartitions the local watch loops per mode
4. deletes orphaned coordination documents

A coordination change stream additionally pushes leadership/membership changes
to all instances with low latency, but it is only an optimization: the
periodic loop is authoritative, so lost events are always healed within one
cycle.

## Resume Strategies

### NONE
- No resume token management
- Change streams start from current position on restart

### PER_EVENT
- Save resume token after every event
- Maximum reliability but higher overhead

### PER_BATCH
- Save resume token after processing each batch
- Balances reliability and performance

### INTERVAL
- Save resume token at regular time intervals
- Configurable via `checkpointInterval`

### Invalid token recovery

Checkpoints are applied with the driver's `startAfter`, so a stream resumes
across an invalidate notification (e.g. the watched collection or database
was dropped and recreated) instead of failing.

When a stream still cannot resume from a stored checkpoint — the token fell
out of the oplog window (`ChangeStreamHistoryLost`, 286), is not part of the
stream's token series (`ChangeStreamFatalError`, 280 /
`NonResumableChangeStreamError`, e.g. checkpoints written by a differently
partitioned AUTO_SCALE pipeline), or is rejected outright
(`InvalidResumeToken`, 260) — the poisoned checkpoint is deleted (across all
hosts) and the stream is automatically restarted without it. Events that
occurred while the token was unusable may be skipped (at-most-once for the
lost window).

## Instance Management

For AUTO_RECOVER and AUTO_SCALE modes, instance liveness comes from heartbeats
in the instance collection (default `_instances`), typically written by the
companion `mongodb-spring-discovery` module. This enables:

- Automatic failover when instances go down (dead heartbeats are actively
  swept and repaired out of every coordination document, without waiting for
  the MongoDB TTL monitor)
- Load balancing across multiple instances
- Coordination between distributed nodes

When the instance collection is empty (no discovery/heartbeat mechanism
active), liveness is unknown and membership repair is skipped; multi-instance
deployments should therefore always run the discovery module.

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.
