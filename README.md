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
    <version>0.0.3</version>
</dependency>
```

Also add the aggregation dependency for pipeline support:

```xml
<dependency>
    <groupId>com.mzinx</groupId>
    <artifactId>mongodb-spring-aggregation</artifactId>
    <version>0.0.3</version>
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

# Maximum timeout for operations in milliseconds (default: 50000)
change-stream.maxTimeout=50000

# Collection name for storing resume tokens (default: _resumeTokens)
change-stream.resumeTokenCollection=_resumeTokens

# Collection name for storing instance information (default: _instances)
change-stream.instanceCollection=_instances
```

## Usage

### Basic Change Stream Setup

```java
@Autowired
private ChangeStreamService<Document> changeStreamService;

// Create a change stream
ChangeStream<Document> changeStream = ChangeStream.of("myChangeStream", Mode.BROADCAST);

// Register and start the change stream
changeStreamService.run("myCollection", changeStream, event -> {
    System.out.println("Change detected: " + event.getOperationType());
    System.out.println("Document: " + event.getFullDocument());
});
```

### Change Stream with Pipeline

```java
List<Bson> pipeline = List.of(
    Aggregates.match(Filters.eq("operationType", "insert"))
);

ChangeStream<Document> changeStream = ChangeStream.of("filteredStream", Mode.BROADCAST)
    .pipeline(pipeline);

changeStreamService.run("myCollection", changeStream, event -> {
    // Only insert operations will be processed
    System.out.println("New document inserted: " + event.getFullDocument());
});
```

### Auto-Recovery Mode

```java
ChangeStream<Document> changeStream = ChangeStream.of("recoverableStream", Mode.AUTO_RECOVER)
    .resumeStrategy(ResumeStrategy.TIME)
    .saveTokenInterval(30000); // Save resume token every 30 seconds

changeStreamService.run("myCollection", changeStream, event -> {
    // Process events with automatic recovery on failures
});
```

### Auto-Scaling Mode

```java
ChangeStream<Document> changeStream = ChangeStream.of("scaledStream", Mode.AUTO_SCALE);

changeStreamService.run("myCollection", changeStream, event -> {
    // Events will be distributed across multiple instances
    // Each instance processes a portion of the changes
});
```

### Advanced Configuration

```java
ChangeStream<Document> changeStream = ChangeStream.of("advancedStream", Mode.BROADCAST)
    .batchSize(500)
    .maxAwaitTime(1000)
    .fullDocument(FullDocument.UPDATE_LOOKUP)
    .fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE)
    .resumeStrategy(ResumeStrategy.BATCH);

changeStreamService.run("myCollection", changeStream, event -> {
    // Advanced change stream with full document lookup
});
```

### Managing Change Streams

```java
// Start a specific change stream
changeStreamService.run("myChangeStream");

// Stop a specific change stream
changeStreamService.stop("myChangeStream");

// Check if running
boolean isRunning = changeStreamService.isRunning("myChangeStream");

// Get all registered change streams
Map<String, ChangeStreamRegistry<?>> streams = changeStreamService.getChangeStreams();
```

## Operation Modes

### BROADCAST Mode
- Basic mode where all events are processed by all registered consumers
- Suitable for simple applications or when all instances need all events

### AUTO_RECOVER Mode
- Provides fault tolerance by automatically recovering failed change streams
- Uses instance tracking to restart streams on different nodes when failures occur
- Requires instance collection monitoring

### AUTO_SCALE Mode
- Distributes change stream processing across multiple instances
- Uses document key hashing to partition events among instances
- Provides horizontal scalability for high-volume change streams

## Resume Strategies

### NONE
- No resume token management
- Change streams start from current position on restart

### EVERY
- Save resume token after every event
- Maximum reliability but higher overhead

### BATCH
- Save resume token after processing each batch
- Balances reliability and performance

### TIME
- Save resume token at regular time intervals
- Configurable via `saveTokenInterval`

## Instance Management

For AUTO_RECOVER and AUTO_SCALE modes, the library maintains an instance collection to track running instances and their assigned change streams. This enables:

- Automatic failover when instances go down
- Load balancing across multiple instances
- Coordination between distributed nodes

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.
