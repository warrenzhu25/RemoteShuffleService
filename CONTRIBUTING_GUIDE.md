# Apache Celeborn Contributor Guide

Welcome to Apache Celeborn! This guide will help you understand the project architecture, development workflow, and contribution process.

## Table of Contents

1. [Project Overview & Architecture](#1-project-overview--architecture)
2. [Getting Started](#2-getting-started)
3. [Development Workflow](#3-development-workflow)
4. [Architecture Deep Dive](#4-architecture-deep-dive)
5. [Metrics & Monitoring](#5-metrics--monitoring)
6. [Configuration System](#6-configuration-system)
7. [Testing Strategy](#7-testing-strategy)
8. [Useful Resources](#8-useful-resources)

---

## 1. Project Overview & Architecture

### What is Apache Celeborn?

Apache Celeborn is a **distributed shuffle service** designed to improve efficiency and elasticity of map-reduce engines (Spark, Flink, MapReduce) by disaggregating compute and storage.

**Core Problem**: Traditional shuffle in distributed computing creates M×N network connections (M mappers, N reducers), leading to inefficiency and resource waste.

**Celeborn's Solution**: Reorganizes shuffle data in a centralized, more efficient way by storing all data with the same `partitionId` in logical `PartitionLocation` objects on dedicated shuffle workers.

### Core Components

Celeborn follows a Master-Worker architecture with specialized client components:

```
┌─────────────────────────────────────────────────────────────┐
│                         Master Cluster                        │
│              (HA via Raft consensus protocol)                 │
│   - Resource management & slot allocation                     │
│   - Shuffle metadata management                               │
│   - Worker health tracking                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ Heartbeat & Slot Requests
                              │
        ┌─────────────────────┴─────────────────────┐
        │                                           │
┌───────▼─────────┐                        ┌───────▼─────────┐
│  Worker Node 1  │                        │  Worker Node N  │
│                 │◄──────Replicate───────►│                 │
│  4 Servers:     │                        │  4 Servers:     │
│  - Controller   │                        │  - Controller   │
│  - Push Server  │                        │  - Push Server  │
│  - Replicate    │                        │  - Replicate    │
│  - Fetch Server │                        │  - Fetch Server │
└────────▲────────┘                        └────────▲────────┘
         │                                          │
         │ Push Data                    Fetch Data │
         │                                          │
┌────────┴──────────────────────────────────────────┴────────┐
│              Application (Spark/Flink/MR)                   │
│                                                              │
│  Driver/JobMaster         Executor/TaskManager              │
│  ┌──────────────┐        ┌──────────────┐                  │
│  │Lifecycle     │        │Shuffle       │                  │
│  │Manager       │        │Client        │                  │
│  └──────────────┘        └──────────────┘                  │
└─────────────────────────────────────────────────────────────┘
```

#### Master
- **HA Support**: Achieves high availability via Apache Ratis (Raft consensus)
- **Slot Allocation**: Allocates shuffle slots using Round Robin or Load Aware strategies
- **Health Monitoring**: Tracks worker health via heartbeats
- **Metadata Management**: Maintains active shuffle metadata

#### Worker
- **4 Dedicated Servers**:
  - **Controller**: Handles control messages (ReserveSlots, CommitFiles, DestroyWorkerSlots)
  - **Push Server**: Receives shuffle data from mappers
  - **Replicate Server**: Receives replica data from peer workers
  - **Fetch Server**: Serves shuffle data to reducers
- **Storage Management**: Multi-tier storage (Memory → Local Disk → HDFS → Cloud)
- **Traffic Control**: Back pressure and congestion control mechanisms
- **Self-Monitoring**: Disk health monitoring and graceful shutdown support

#### Client
- **LifecycleManager** (resides in Driver/JobMaster):
  - One per application
  - Registers shuffles and manages metadata
  - Requests slots from Master
  - Handles partition splits and revive operations

- **ShuffleClient** (resides in Executor/TaskManager):
  - One per process
  - Pushes shuffle data to workers
  - Reads shuffle data for reduce tasks
  - Supports compression (zstd, lz4) and data batching

### Data Flow: Shuffle Lifecycle

1. **Registration**: Mappers lazily register shuffle with LifecycleManager
2. **Slot Allocation**: LifecycleManager requests slots from Master; Master allocates slots on Workers
3. **Reservation**: Workers reserve slots and create files
4. **Push**: Mappers push data to specified workers
5. **Replication**: Workers merge and replicate data to peer workers for fault tolerance
6. **Flush**: Workers flush data to disk periodically
7. **Commit**: On mapper completion, workers commit files
8. **Fetch**: Reducers request file locations and read shuffle data

### Module Structure

```
celeborn/
├── common/              # Shared utilities, configs, metrics, RPC, protocols
├── master/              # Master service implementation
├── worker/              # Worker service implementation
├── client/              # Core client library (LifecycleManager, ShuffleClient)
├── client-spark/        # Spark integration (2.4, 3.0-4.0)
├── client-flink/        # Flink integration (1.16-2.1)
├── client-mr/           # MapReduce integration
├── client-tez/          # Tez integration (experimental)
├── service/             # Metrics system, HTTP servers
├── spi/                 # Service Provider Interfaces
├── cli/                 # Command-line tools
├── web/                 # Web UI
├── tests/               # Integration tests (spark-it, flink-it, mr-it, k8s-it)
├── multipart-uploader/  # Cloud storage multipart upload support
└── docs/                # Comprehensive documentation
```

---

## 2. Getting Started

### Prerequisites

- **Java**: JDK 8, 11, 17, or 21
- **Build Tools**: Maven 3.9.9+ or SBT
- **Scala**: 2.11, 2.12, or 2.13 (managed by build tool)
- **Git**: For version control

### Clone the Repository

```bash
git clone https://github.com/apache/celeborn.git
cd celeborn
```

### Building the Project

Celeborn supports both Maven and SBT build systems.

#### Maven Build (Primary)

```bash
# Basic build
./build/mvn clean package -DskipTests

# Build with specific Spark version
./build/make-distribution.sh -Pspark-3.3

# Build with multiple profiles
./build/make-distribution.sh -Pspark-3.5 -Pflink-1.20

# Build with cloud storage support
./build/make-distribution.sh -Pspark-3.3 -Paws
```

#### SBT Build (Available since v0.4.0)

```bash
# Basic build
./build/sbt clean package

# Create distribution
./build/make-distribution.sh --sbt-enabled

# Run specific module tests
./build/sbt "project common" test
```

#### Available Build Profiles

**Compute Engines**:
- Spark: `-Pspark-2.4`, `-Pspark-3.0`, `-Pspark-3.1`, `-Pspark-3.2`, `-Pspark-3.3`, `-Pspark-3.4`, `-Pspark-3.5`, `-Pspark-4.0`
- Flink: `-Pflink-1.16`, `-Pflink-1.17`, `-Pflink-1.18`, `-Pflink-1.19`, `-Pflink-1.20`, `-Pflink-2.0`, `-Pflink-2.1`
- MapReduce: `-Pmr`
- Tez: `-Ptez`

**Cloud Storage**:
- `-Paws`: Amazon S3 support
- `-Paliyun`: Alibaba Cloud OSS support

**Java Versions**:
- `-Pjdk-8`, `-Pjdk-11`, `-Pjdk-17`, `-Pjdk-21`

### Running Tests

```bash
# Run all tests
./build/mvn test

# Run tests for specific profile
./build/mvn -Pspark-3.3 test

# Run tests for specific module
./build/mvn -pl common test

# Run specific test suite
./build/mvn -pl common -Dtest=CelebornConfSuite test

# Skip tests during build
./build/mvn clean package -DskipTests
```

---

## 3. Development Workflow

### Before You Start

1. **Join the Community**:
   - Slack: [Join Apache Celeborn Slack](https://join.slack.com/t/apachecelebor-kw08030/shared_invite/zt-1ju3hd5j8-4Z5keMdzpcVMspe4UJzF4Q)
   - Mailing Lists: [Subscribe to dev@celeborn.apache.org](https://celeborn.apache.org/community/)

2. **File a JIRA Ticket**:
   - Browse existing issues: [JIRA Project](https://issues.apache.org/jira/projects/CELEBORN)
   - Create a new ticket for your feature or bug fix
   - Use format: `[CELEBORN-XXX] Brief description`

### Code Style and Formatting

Celeborn uses **Spotless** for automatic code formatting and **Scalafmt** for Scala code.

#### Format Your Code

```bash
# Format all code (Java + Scala)
./dev/reformat

# Format web module
./dev/reformat --web

# Check formatting without making changes
./build/mvn spotless:check
```

#### Scala Style Rules

Key rules enforced by Scalastyle:
- **Maximum 100 characters per line**
- **Files must end with newline**
- **No trailing whitespace**
- **Use `Utils.classForName` instead of `Class.forName`** (or wrap with scalastyle comments if necessary)

Check Scala style:
```bash
# Check specific module
./build/sbt "core/scalastyle"

# Check all modules
./build/sbt scalastyle
```

Fix common formatting issues:
```bash
# Remove trailing whitespace
find . -name "*.scala" -exec sed -i '' 's/[[:space:]]*$//' {} \;

# Add final newlines
find . -name "*.scala" -exec sh -c 'if [ ! -z "$(tail -c1 "$1")" ]; then echo >> "$1"; fi' _ {} \;
```

### Making Changes

1. **Create a Feature Branch**:
   ```bash
   git checkout -b feature/CELEBORN-XXX-brief-description
   ```

2. **Study Existing Code**:
   - Find similar implementations in the codebase
   - Follow established patterns and conventions
   - Understand the existing architecture before adding new features

3. **Write Tests First** (TDD approach):
   - Write failing tests that define expected behavior
   - Implement minimal code to pass tests
   - Refactor for clarity and performance

4. **Implement Your Changes**:
   - Follow existing code patterns
   - Keep commits atomic and focused
   - Write clear, self-documenting code

5. **Run Tests and Checks**:
   ```bash
   # Format code
   ./dev/reformat

   # Run tests
   ./build/mvn test

   # Check licenses
   ./build/mvn apache-rat:check

   # Check style
   ./build/sbt scalastyle
   ```

### Configuration Changes

If you modify configuration options, update the configuration test:

```bash
UPDATE=1 build/mvn clean test -pl common -am -Dtest=none \
  -DwildcardSuites=org.apache.celeborn.ConfigurationSuite
```

### RPC Development Guidelines

When working with RPC messages:

- **Use raw PB message case**: Prefer `RegisterWorker`, `RegisterWorkerResponse` (not wrapped types)
- **Avoid `map` types in Protobuf**: Use `repeated` instead to avoid reflection overhead
- **Update both client and server**: Ensure protocol changes are backward compatible when possible

Example:
```protobuf
// Good: Use repeated
message Config {
  repeated ConfigEntry entries = 1;
}

message ConfigEntry {
  string key = 1;
  string value = 2;
}

// Avoid: map type
message Config {
  map<string, string> entries = 1;  // Avoid this
}
```

### Submitting a Pull Request

1. **Push Your Branch**:
   ```bash
   git push origin feature/CELEBORN-XXX-brief-description
   ```

2. **Create PR**:
   - Go to [GitHub](https://github.com/apache/celeborn)
   - Create Pull Request from your branch to `main`
   - Use title format: `[CELEBORN-XXX] Brief description`
   - Fill out the PR template completely

3. **PR Checklist**:
   - [ ] Code is formatted (`./dev/reformat`)
   - [ ] Tests pass (`./build/mvn test`)
   - [ ] New tests added for new functionality
   - [ ] Scalastyle violations fixed
   - [ ] License headers present on new files
   - [ ] JIRA ticket linked in PR description
   - [ ] Documentation updated if needed

4. **Code Review**:
   - Address reviewer feedback promptly
   - Keep discussions constructive and professional
   - Be open to suggestions and alternative approaches

5. **Merging**:
   - Committers will merge your PR after approval
   - PRs require at least one approving review

### Dependency Management

When adding or updating dependencies:

1. **Update `LICENSE-binary`**: Add licenses for new dependencies
2. **Check compatibility**: Ensure dependencies work across all supported versions
3. **Minimize dependencies**: Only add truly necessary dependencies
4. **Document changes**: Explain why the dependency is needed

---

## 4. Architecture Deep Dive

### Master Component

**Location**: `master/src/main/scala/org/apache/celeborn/service/deploy/master/Master.scala`

#### Key Responsibilities

1. **Cluster Resource Management**:
   - Tracks all workers and their health status
   - Monitors available slots and disk space
   - Handles worker registration, heartbeats, and decommissioning

2. **Shuffle Metadata Management**:
   - Maintains active shuffle metadata
   - Tracks application lifecycle
   - Estimates partition sizes for optimization

3. **High Availability**:
   - Uses Apache Ratis for Raft consensus
   - Automatic leader election
   - State machine replication across master nodes

4. **Slot Allocation**:
   - **Round Robin**: Simple equal distribution
   - **Load Aware**: Allocates based on disk flush/fetch performance metrics

#### Master Metrics

Key metrics exposed by Master:
- `WorkerCount`, `LostWorkerCount`, `ExcludedWorkerCount`, `AvailableWorkerCount`
- `RegisteredShuffleCount`, `ShuffleTotalCount`, `RunningApplicationCount`
- `DeviceCelebornFreeBytes`, `DeviceCelebornTotalBytes`
- `ActiveShuffleSize`, `ActiveShuffleFileCount`
- `OfferSlotsTime`, `PartitionSize`
- `IsActiveMaster`, `RatisApplyCompletedIndex` (HA metrics)

### Worker Component

**Location**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/Worker.scala`

#### Key Responsibilities

1. **Data Storage**:
   - Receives shuffle data via Push Server
   - Stores data in multi-tier storage (Memory → Local → HDFS → Cloud)
   - Manages partition metadata

2. **Four Dedicated Servers**:
   ```
   Controller (Port: celeborn.worker.controller.port)
     └─> Handles: ReserveSlots, CommitFiles, DestroyWorkerSlots

   Push Server (Port: celeborn.worker.push.port)
     └─> Handles: PushData, PushMergedData

   Replicate Server (Port: celeborn.worker.replicate.port)
     └─> Handles: Replica data from peer workers

   Fetch Server (Port: celeborn.worker.fetch.port)
     └─> Handles: ChunkFetchRequest from reducers
   ```

3. **Storage Management**:
   - **Flusher**: Async flushing to different storage tiers
   - **DeviceMonitor**: Monitors disk health and capacity
   - **StorageManager**: Coordinates multi-tier storage
   - **PartitionMetaHandler**: Manages partition metadata

4. **Traffic Control**:
   - Back pressure when buffers are full
   - Congestion control to prevent network overload
   - Credit-based flow control

#### Worker Storage Architecture

**Storage Hierarchy**:
```
┌─────────────────────────────────────────────────┐
│              Memory Buffer                      │
│        (celeborn.worker.flusher.buffer)         │
└──────────────────┬──────────────────────────────┘
                   │ Flush
                   ▼
┌─────────────────────────────────────────────────┐
│              Local Disk                         │
│      (celeborn.worker.storage.dirs)             │
│    SSD/HDD with health monitoring               │
└──────────────────┬──────────────────────────────┘
                   │ Evict/Archive
                   ▼
┌─────────────────────────────────────────────────┐
│          Distributed Storage                    │
│  HDFS / S3 / OSS / Azure Blob                   │
│  (celeborn.storage.hdfs.dir)                    │
└─────────────────────────────────────────────────┘
```

**Storage Implementation**:
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/StorageManager.scala`
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/Flusher.scala`
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/DeviceMonitor.scala`

#### Worker Metrics

Key metrics exposed by Worker:
- Stream: `OpenStreamSuccessCount`, `FetchChunkSuccessCount`
- Write: `WriteDataSuccessCount`, `WriteDataFailCount`, `WriteDataHardSplitCount`
- Replicate: `ReplicateDataFailCount`, `ReplicateDataTimeoutCount`
- Storage (per type): `LocalFlushCount`, `HdfsFlushCount`, `OssFlushCount`, `S3FlushCount`
- Timers: `CommitFilesTime`, `FlushLocalDataTime`, `FlushHdfsDataTime`
- Resources: `SlotsAllocated`, `ActiveConnectionCount`

### Client Component

#### LifecycleManager

**Location**: `client/src/main/scala/org/apache/celeborn/client/LifecycleManager.scala`

**Deployment**: One per application in Driver/JobMaster

**Responsibilities**:
- Register shuffle with Master
- Request and manage slot allocations
- Handle partition splits (Revive operation)
- Track mapper and stage completion
- Provide reducer file groups
- Send heartbeats to Master

**Key Operations**:
```scala
// Register a new shuffle
registerShuffle(shuffleId: Int, numMappers: Int, numPartitions: Int)

// Request slots for a shuffle
requestSlots(shuffleId: Int)

// Handle partition split when files grow too large
revive(shuffleId: Int, partitionId: Int)

// Notify mapper completion
mapperEnd(shuffleId: Int, mapperId: Int)

// Get file locations for reducers
getReducerFileGroup(shuffleId: Int, partitionId: Int)
```

#### ShuffleClient

**Location**: `client/src/main/scala/org/apache/celeborn/client/ShuffleClient.scala`

**Deployment**: One per Executor/TaskManager process

**Responsibilities**:
- Push shuffle data to workers
- Read shuffle data for reduce tasks
- Manage data compression (zstd, lz4)
- Batch small data via `PushMergedData`
- Handle async push via `DataPusher`

**Push Flow**:
```
Mapper → ShuffleClient.pushData()
            ↓
        DataPusher (async batching)
            ↓
        Network Transport
            ↓
        Worker Push Server
```

**Fetch Flow**:
```
Reducer → ShuffleClient.readPartition()
             ↓
         Fetch Server Request
             ↓
         Worker Fetch Server
             ↓
         ChunkFetchHandler
             ↓
         Streamed Data
```

### RPC Framework

**Location**: `common/src/main/scala/org/apache/celeborn/common/rpc/`

Celeborn implements a custom RPC framework built on Netty:

- **RpcEnv**: Environment for RPC communication
- **RpcEndpoint**: Server-side endpoint that receives messages
- **RpcEndpointRef**: Client-side reference to send messages
- **Transport Layer**: `common/src/main/scala/org/apache/celeborn/common/network/`

**Message Types**:
- **One-way messages**: Fire and forget (via `send()`)
- **RPC calls**: Request-response (via `ask()`)
- **Streamed data**: Large data transfers via `TransportClient`

### Data Partitioning

**Two Partition Types**:

1. **ReducePartition** (for Spark):
   - Data organized in 8MB chunks
   - In-memory index for fast lookups
   - Sorted by partition ID

2. **MapPartition** (for Flink):
   - Data organized in 64MB regions
   - Each region contains data from single mapper
   - Sorted by partition ID within region

**Partition Split**:
When files grow too large or disk space is low:
- Original partition marked as split
- New partition location allocated
- Subsequent data written to new location
- Reducers read from multiple locations

---

## 5. Metrics & Monitoring

Celeborn provides comprehensive metrics for monitoring cluster health and performance.

### Metrics Architecture

**Based on**: [Dropwizard Metrics Library 4.2.25](https://metrics.dropwizard.io/4.2.0/)

**Core Class**: `MetricsSystem` (`service/src/main/scala/org/apache/celeborn/common/metrics/MetricsSystem.scala`)

**Components**:
- **Sources**: Generate metrics data
- **Sinks**: Export metrics to various destinations

### Metric Types

- **Gauge**: Instant value (e.g., current worker count)
- **Counter**: Incrementing value (e.g., total shuffles)
- **Timer**: Timing measurements (e.g., flush duration)
- **Histogram**: Statistical distribution (e.g., partition sizes)
- **Meter**: Rate measurements (e.g., operations per second)

### Configuration

Configure via `$CELEBORN_HOME/conf/metrics.properties` or dynamic config with prefix `celeborn.metrics.conf.`:

```properties
# Enable metrics
celeborn.metrics.enabled=true

# Prometheus endpoint
*.sink.prometheusServlet.class=org.apache.celeborn.common.metrics.sink.PrometheusServlet
celeborn.metrics.prometheus.path=/metrics/prometheus

# JSON endpoint
*.sink.jsonServlet.class=org.apache.celeborn.common.metrics.sink.JsonServlet
celeborn.metrics.json.path=/metrics/json

# Logger sink (logs metrics periodically)
*.sink.loggerSink.class=org.apache.celeborn.common.metrics.sink.LoggerSink
*.sink.loggerSink.period=60
*.sink.loggerSink.unit=seconds

# CSV export
*.sink.csv.class=org.apache.celeborn.common.metrics.sink.CsvSink
*.sink.csv.directory=/tmp/celeborn-metrics
*.sink.csv.period=60

# Graphite export
*.sink.graphite.class=org.apache.celeborn.common.metrics.sink.GraphiteSink
*.sink.graphite.host=graphite.example.com
*.sink.graphite.port=2003
*.sink.graphite.period=60
```

### Available Sinks

1. **PrometheusServlet**: Serves metrics in Prometheus format via HTTP
2. **JsonServlet**: Serves metrics in JSON format via HTTP
3. **LoggerSink**: Periodically logs metrics to file
4. **CsvSink**: Exports metrics to CSV files
5. **GraphiteSink**: Sends metrics to Graphite server

### Key Metrics by Component

#### Master Metrics (MasterSource)

```
# Worker Management
celeborn.master.WorkerCount                    # Total registered workers
celeborn.master.LostWorkerCount                # Lost workers
celeborn.master.ExcludedWorkerCount            # Excluded workers
celeborn.master.ShutdownWorkerCount            # Gracefully shutdown workers
celeborn.master.AvailableWorkerCount           # Available for allocation
celeborn.master.DecommissionWorkerCount        # Being decommissioned

# Shuffle Tracking
celeborn.master.RegisteredShuffleCount         # Currently active shuffles
celeborn.master.ShuffleTotalCount              # Total shuffles handled
celeborn.master.ShuffleFallbackCount           # Shuffles that fell back
celeborn.master.RunningApplicationCount        # Active applications
celeborn.master.ApplicationTotalCount          # Total applications
celeborn.master.ApplicationFallbackCount       # Applications that fell back

# Capacity
celeborn.master.DeviceCelebornFreeBytes        # Free disk space across cluster
celeborn.master.DeviceCelebornTotalBytes       # Total disk space
celeborn.master.ActiveShuffleSize              # Total active shuffle data size
celeborn.master.ActiveShuffleFileCount         # Number of active shuffle files

# Performance
celeborn.master.OfferSlotsTime                 # Time to allocate slots
celeborn.master.PartitionSize                  # Estimated partition sizes

# HA Status
celeborn.master.IsActiveMaster                 # 1 if active, 0 if standby
celeborn.master.RatisApplyCompletedIndex       # Raft log index
celeborn.master.RatisApplyCompletedIndexDiff   # Lag between leader and follower
```

#### Worker Metrics (WorkerSource)

```
# Stream Operations
celeborn.worker.OpenStreamSuccessCount         # Successful stream opens
celeborn.worker.OpenStreamFailCount            # Failed stream opens
celeborn.worker.FetchChunkSuccessCount         # Successful chunk fetches
celeborn.worker.FetchChunkFailCount            # Failed chunk fetches

# Write Operations
celeborn.worker.WriteDataSuccessCount          # Successful writes
celeborn.worker.WriteDataFailCount             # Failed writes
celeborn.worker.WriteDataHardSplitCount        # Hard partition splits

# Replication
celeborn.worker.ReplicateDataFailCount         # Failed replications
celeborn.worker.ReplicateDataTimeoutCount      # Replication timeouts

# Storage Operations (per storage type: Local, Hdfs, Oss, S3)
celeborn.worker.LocalFlushCount                # Local flushes
celeborn.worker.LocalFlushSize                 # Bytes flushed to local
celeborn.worker.HdfsFlushCount                 # HDFS flushes
celeborn.worker.HdfsFlushSize                  # Bytes flushed to HDFS
celeborn.worker.OssFlushCount                  # OSS flushes
celeborn.worker.S3FlushCount                   # S3 flushes

# Timings
celeborn.worker.CommitFilesTime                # Time to commit files
celeborn.worker.ReserveSlotsTime               # Time to reserve slots
celeborn.worker.FlushLocalDataTime             # Time to flush to local disk
celeborn.worker.FlushHdfsDataTime              # Time to flush to HDFS
celeborn.worker.FlushOssDataTime               # Time to flush to OSS
celeborn.worker.FlushS3DataTime                # Time to flush to S3

# Resources
celeborn.worker.SlotsAllocated                 # Number of allocated slots
celeborn.worker.ActiveConnectionCount          # Active network connections
```

#### System Metrics

```
# JVM
celeborn.jvm.heap.used                         # Heap memory used
celeborn.jvm.heap.committed                    # Heap memory committed
celeborn.jvm.non-heap.used                     # Non-heap memory used
celeborn.jvm.total.used                        # Total memory used

# CPU
celeborn.jvm.JVMCPUTime                        # CPU time used by JVM
celeborn.system.LastMinuteSystemLoad           # System load average
celeborn.system.AvailableProcessors            # Available CPU cores

# Threads
celeborn.jvm.thread.count                      # Active threads
celeborn.jvm.thread.daemon.count               # Daemon threads
celeborn.jvm.thread.blocked.count              # Blocked threads
```

### Monitoring Endpoints

Once Celeborn services are running, access metrics via HTTP:

```bash
# Prometheus format (for Prometheus scraping)
curl http://<master-host>:<http-port>/metrics/prometheus
curl http://<worker-host>:<http-port>/metrics/prometheus

# JSON format (human-readable)
curl http://<master-host>:<http-port>/metrics/json
curl http://<worker-host>:<http-port>/metrics/json

# Configuration
curl http://<master-host>:<http-port>/conf
curl http://<worker-host>:<http-port>/conf

# Dynamic configuration
curl http://<master-host>:<http-port>/listDynamicConfigs
```

Default HTTP ports:
- Master: `celeborn.master.http.port` (default: 9098)
- Worker: `celeborn.worker.http.port` (default: 9096)

### Custom Metrics Implementation

To add custom metrics in your code:

```scala
// In a Source class
class CustomSource(conf: CelebornConf) extends AbstractSource(conf, Role.WORKER) {
  override val sourceName = "custom"

  // Add a counter
  addCounter("myCounter")

  // Add a gauge with dynamic value
  addGauge("myGauge", () => computeCurrentValue())

  // Add a timer
  addTimer("myTimer")

  // Record timer execution
  def recordOperation(): Unit = {
    val timer = metricRegistry.timer("myTimer")
    val context = timer.time()
    try {
      // Perform operation
    } finally {
      context.stop()
    }
  }
}
```

### Prometheus Integration Example

1. **Configure Prometheus** (`prometheus.yml`):
   ```yaml
   scrape_configs:
     - job_name: 'celeborn-master'
       static_configs:
         - targets: ['master-host:9098']
       metrics_path: '/metrics/prometheus'
       scrape_interval: 15s

     - job_name: 'celeborn-workers'
       static_configs:
         - targets: ['worker1:9096', 'worker2:9096', 'worker3:9096']
       metrics_path: '/metrics/prometheus'
       scrape_interval: 15s
   ```

2. **Query in Prometheus**:
   ```promql
   # Worker count
   celeborn_master_WorkerCount

   # Flush rate per worker
   rate(celeborn_worker_LocalFlushCount[5m])

   # Average slot allocation time
   rate(celeborn_master_OfferSlotsTime_sum[5m]) / rate(celeborn_master_OfferSlotsTime_count[5m])
   ```

3. **Grafana Dashboards**: Create custom dashboards visualizing:
   - Cluster capacity and utilization
   - Shuffle throughput
   - Worker health status
   - Performance metrics (latency, throughput)

---

## 6. Configuration System

Celeborn provides a flexible, hierarchical configuration system supporting both static and dynamic configurations.

### Configuration Architecture

**Core Class**: `CelebornConf` (`common/src/main/scala/org/apache/celeborn/common/CelebornConf.scala`)

**Configuration Hierarchy** (highest to lowest precedence):
1. **TENANT_USER**: Tenant-specific user configuration
2. **TENANT**: Tenant-level configuration
3. **SYSTEM**: Dynamic system-level configuration
4. **Static**: From `celeborn-defaults.conf` and system properties
5. **Default**: Built-in defaults

### Configuration Files

**Templates in `$CELEBORN_HOME/conf/`**:

1. **celeborn-defaults.conf.template**: Main configuration file
   ```bash
   cp conf/celeborn-defaults.conf.template conf/celeborn-defaults.conf
   ```

2. **celeborn-env.sh.template**: Environment variables (memory, Java opts)
   ```bash
   cp conf/celeborn-env.sh.template conf/celeborn-env.sh
   ```

3. **metrics.properties.template**: Metrics sinks configuration
   ```bash
   cp conf/metrics.properties.template conf/metrics.properties
   ```

4. **log4j2.xml.template**: Logging configuration
   ```bash
   cp conf/log4j2.xml.template conf/log4j2.xml
   ```

5. **dynamicConfig.yaml.template**: Dynamic multi-tenant configuration
   ```bash
   cp conf/dynamicConfig.yaml.template conf/dynamicConfig.yaml
   ```

### Static Configuration

Loaded at startup from:
- `$CELEBORN_HOME/conf/celeborn-defaults.conf`
- System properties with `celeborn.*` prefix (via `-Dceleborn.key=value`)

**Example Configuration** (`celeborn-defaults.conf`):

```properties
# Master Configuration
celeborn.master.host localhost
celeborn.master.port 9097

# Worker Configuration
celeborn.worker.storage.dirs /mnt/disk1:disktype=SSD,/mnt/disk2:disktype=HDD
celeborn.worker.flusher.buffer.size 256k
celeborn.worker.monitor.disk.enabled true

# Network
celeborn.network.io.numConnectionsPerPeer 2
celeborn.network.io.connectTimeout 10s

# Data Configuration
celeborn.push.replicate.enabled true
celeborn.push.maxReqsInFlight 32

# Shuffle
celeborn.shuffle.partitionType reduce
celeborn.shuffle.compression.codec lz4
```

### Dynamic Configuration

Can be updated at runtime without service restart. Supports multi-tenancy.

**Backend Options**:
- **FileSystem** (`FS`): YAML file-based
- **Database** (`DB`): JDBC-based
- **Custom**: Implement `ConfigService` interface

#### FileSystem Backend

**Configuration**:
```properties
celeborn.dynamicConfig.store.backend FS
celeborn.dynamicConfig.store.fs.path /path/to/dynamicConfig.yaml
```

**Example `dynamicConfig.yaml`**:
```yaml
# System-level configuration (applies to all)
- level: SYSTEM
  config:
    celeborn.worker.flusher.buffer.size: 256k
    celeborn.push.maxReqsInFlight: 32

# Tenant-level configuration
- level: TENANT
  tenantId: tenant-001
  config:
    celeborn.worker.flusher.buffer.size: 512k
    celeborn.quota.tenant.diskBytesWritten: 10T

  # User-level configuration within tenant
  users:
    - name: user-001
      config:
        celeborn.worker.flusher.buffer.size: 128k
        celeborn.quota.user.diskBytesWritten: 1T

# Another tenant
- level: TENANT
  tenantId: tenant-002
  config:
    celeborn.worker.flusher.buffer.size: 1m
```

**Precedence Example**:
- `tenant-001/user-001` gets: `128k` (TENANT_USER level)
- `tenant-001/user-002` gets: `512k` (TENANT level)
- `tenant-002/user-001` gets: `1m` (TENANT level)
- `tenant-003/user-001` gets: `256k` (SYSTEM level)

#### Database Backend

**Configuration**:
```properties
celeborn.dynamicConfig.store.backend DB
celeborn.dynamicConfig.store.db.fetch.interval 30s
celeborn.dynamicConfig.store.db.hikari.driverClassName com.mysql.jdbc.Driver
celeborn.dynamicConfig.store.db.hikari.jdbcUrl jdbc:mysql://localhost:3306/celeborn_config
celeborn.dynamicConfig.store.db.hikari.username celeborn_user
celeborn.dynamicConfig.store.db.hikari.password your_password
```

**Database Schema**:
SQL scripts available in `service/src/main/resources/sql/mysql/`:
- `celeborn_cluster_info`: Cluster metadata
- `celeborn_cluster_system_config`: System-level config
- `celeborn_cluster_tenant_config`: Tenant and user configs

### Common Configuration Patterns

#### Single Master Setup

```properties
# celeborn-defaults.conf
celeborn.master.host master-hostname
celeborn.master.port 9097
celeborn.master.endpoints master-hostname:9097
```

#### High Availability Master Cluster

```properties
# celeborn-defaults.conf
celeborn.master.ha.enabled true
celeborn.master.ha.node.id 1
celeborn.master.ha.node.1.host master1-hostname
celeborn.master.ha.node.1.port 9097
celeborn.master.ha.node.1.ratis.port 9872
celeborn.master.ha.node.2.host master2-hostname
celeborn.master.ha.node.2.port 9097
celeborn.master.ha.node.2.ratis.port 9872
celeborn.master.ha.node.3.host master3-hostname
celeborn.master.ha.node.3.port 9097
celeborn.master.ha.node.3.ratis.port 9872
celeborn.master.ha.ratis.raft.server.storage.dir /mnt/disk1/ratis_data

# On each master, set different node.id:
# Master 1: celeborn.master.ha.node.id = 1
# Master 2: celeborn.master.ha.node.id = 2
# Master 3: celeborn.master.ha.node.id = 3
```

#### Worker with Multi-tier Storage

```properties
# Local disk
celeborn.worker.storage.dirs /mnt/disk1:disktype=SSD,/mnt/disk2:disktype=HDD,/mnt/disk3:disktype=HDD

# HDFS
celeborn.storage.availableTypes LOCAL,HDFS
celeborn.storage.hdfs.dir hdfs://namenode:8020/celeborn
celeborn.worker.flusher.hdfs.buffer.size 4m

# HDFS Kerberos
celeborn.storage.hdfs.kerberos.principal celeborn@EXAMPLE.COM
celeborn.storage.hdfs.kerberos.keytab /path/to/celeborn.keytab
```

#### Cloud Storage (S3)

```properties
# S3 configuration
celeborn.storage.availableTypes S3
celeborn.storage.s3.bucket my-shuffle-bucket
celeborn.storage.s3.dir s3://my-shuffle-bucket/celeborn/
celeborn.storage.s3.region us-west-2
celeborn.storage.s3.access.key <ACCESS_KEY>
celeborn.storage.s3.secret.key <SECRET_KEY>
celeborn.worker.flusher.s3.buffer.size 4m
```

#### Cloud Storage (OSS - Alibaba Cloud)

```properties
# OSS configuration
celeborn.storage.availableTypes OSS
celeborn.storage.oss.bucket my-shuffle-bucket
celeborn.storage.oss.dir oss://my-shuffle-bucket/celeborn/
celeborn.storage.oss.access.key <ACCESS_KEY>
celeborn.storage.oss.secret.key <SECRET_KEY>
celeborn.storage.oss.endpoint oss-cn-beijing.aliyuncs.com
celeborn.worker.flusher.oss.buffer.size 4m
```

#### Memory Configuration

**In `celeborn-env.sh`**:
```bash
# Master memory
CELEBORN_MASTER_MEMORY=4g
CELEBORN_MASTER_JAVA_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Worker memory
CELEBORN_WORKER_MEMORY=2g
CELEBORN_WORKER_OFFHEAP_MEMORY=8g
CELEBORN_WORKER_JAVA_OPTS="-XX:+UseG1GC -XX:MaxDirectMemorySize=8g"
```

#### Performance Tuning

```properties
# Push data configuration
celeborn.push.replicate.enabled true
celeborn.push.maxReqsInFlight 64
celeborn.push.buffer.size 64k
celeborn.push.queue.capacity 512

# Flusher configuration
celeborn.worker.flusher.threads 4
celeborn.worker.flusher.buffer.size 512k
celeborn.worker.flusher.hdfs.buffer.size 4m

# Network configuration
celeborn.network.io.numConnectionsPerPeer 4
celeborn.network.io.connectTimeout 30s
celeborn.network.io.connectionTimeout 120s

# Compression
celeborn.shuffle.compression.codec lz4
celeborn.shuffle.compression.zstd.level 3
```

### Configuration Management via REST API

**List current configuration**:
```bash
curl http://master-host:9098/conf
curl http://worker-host:9096/conf
```

**List dynamic configurations**:
```bash
curl http://master-host:9098/listDynamicConfigs
```

### Transport Module Configuration

Celeborn supports module-specific configuration overrides with fallback:

```
celeborn.<module>.<config> → celeborn.<parent-module>.<config> → celeborn.<config>
```

**Example**:
```properties
# Global network timeout
celeborn.network.io.connectTimeout 10s

# Override for push server
celeborn.push.io.connectTimeout 30s

# Override for fetch server
celeborn.fetch.io.connectTimeout 60s
```

**Internal modules with predefined fallbacks**:
- `push` → `data` → base
- `fetch` → `data` → base
- `replicate` → `data` → base

---

## 7. Testing Strategy

Celeborn employs comprehensive testing at multiple levels to ensure reliability and correctness.

### Test Structure

```
celeborn/
├── */src/test/                    # Unit tests (in each module)
│   ├── java/                      # Java unit tests
│   └── scala/                     # Scala unit tests
└── tests/                         # Integration tests
    ├── spark-it/                  # Spark integration tests
    ├── flink-it/                  # Flink integration tests
    ├── mr-it/                     # MapReduce integration tests
    ├── kubernetes-it/             # Kubernetes integration tests
    └── tez-it/                    # Tez integration tests
```

### Unit Tests

Located in each module's `src/test/` directory.

**Running Unit Tests**:
```bash
# All unit tests
./build/mvn test

# Specific module
./build/mvn -pl common test
./build/mvn -pl master test
./build/mvn -pl worker test

# Specific test suite
./build/mvn -pl common -Dtest=CelebornConfSuite test

# Wildcard test suites
./build/mvn -pl common -DwildcardSuites=org.apache.celeborn.ConfigurationSuite test

# Skip tests
./build/mvn package -DskipTests

# SBT tests
./build/sbt test
./build/sbt "project common" test
```

**Test Framework**:
- **ScalaTest**: Primary testing framework for Scala
- **JUnit**: For Java tests
- **Mockito**: For mocking

**Example Test Structure**:
```scala
class MyComponentSuite extends CelebornFunSuite {

  test("should handle normal case correctly") {
    // Arrange
    val component = new MyComponent()

    // Act
    val result = component.process(input)

    // Assert
    assert(result === expected)
  }

  test("should throw exception for invalid input") {
    val component = new MyComponent()

    intercept[IllegalArgumentException] {
      component.process(invalidInput)
    }
  }
}
```

### Integration Tests

**Spark Integration Tests** (`tests/spark-it`):
```bash
# Run all Spark integration tests
./build/mvn -pl tests/spark-it test -Pspark-3.3

# Run specific Spark version
./build/mvn -pl tests/spark-it test -Pspark-3.5

# Run specific test
./build/mvn -pl tests/spark-it -Dtest=CelebornShuffleSuite test -Pspark-3.3
```

**Flink Integration Tests** (`tests/flink-it`):
```bash
# Run all Flink integration tests
./build/mvn -pl tests/flink-it test -Pflink-1.18

# Run specific Flink version
./build/mvn -pl tests/flink-it test -Pflink-1.20
```

**MapReduce Integration Tests** (`tests/mr-it`):
```bash
./build/mvn -pl tests/mr-it test -Pmr
```

**Kubernetes Integration Tests** (`tests/kubernetes-it`):
```bash
./build/mvn -pl tests/kubernetes-it test
```

### Continuous Integration

**GitHub Actions Workflows** (`.github/workflows/`):

1. **maven.yml**: Main build and test workflow
   - Tests across Java 8, 11, 17
   - Tests multiple Spark versions (2.4, 3.0, 3.3, 3.5, 4.0)
   - Tests multiple Flink versions (1.17, 1.18, 1.20)
   - Runs on: push to main, pull requests

2. **sbt.yml**: SBT build verification
   - Ensures SBT build compatibility
   - Tests Java 8, 11, 17

3. **integration.yml**: Integration test suite
   - Spark, Flink, MR integration tests
   - End-to-end scenarios

4. **style.yml**: Code style checks
   - Spotless formatting
   - Scalastyle
   - License headers (Apache RAT)

5. **codecov.yml**: Code coverage
   - Uploads coverage to Codecov
   - Tracks coverage trends

**Test Coverage**:
- Target: Maintain high coverage for critical components
- View coverage: [Codecov Dashboard](https://app.codecov.io/gh/apache/celeborn)

### Testing Best Practices

1. **Write Tests First** (TDD):
   ```scala
   // 1. Write failing test
   test("new feature should work") {
     val result = newFeature.execute()
     assert(result === expected)
   }

   // 2. Implement minimal code to pass
   // 3. Refactor
   ```

2. **Test Naming**:
   - Use descriptive names: `"should X when Y"`
   - Examples:
     - `"should allocate slots when workers are available"`
     - `"should throw exception when partition ID is invalid"`

3. **Test Organization**:
   ```scala
   class ComponentSuite extends CelebornFunSuite {
     // Setup
     var component: Component = _

     override def beforeEach(): Unit = {
       component = new Component()
     }

     // Happy path tests
     test("should handle normal case") { ... }

     // Edge cases
     test("should handle empty input") { ... }
     test("should handle maximum size input") { ... }

     // Error cases
     test("should throw exception for invalid input") { ... }

     // Cleanup
     override def afterEach(): Unit = {
       component.cleanup()
     }
   }
   ```

4. **Mock External Dependencies**:
   ```scala
   val mockRpcEnv = mock[RpcEnv]
   when(mockRpcEnv.setupEndpoint(...)).thenReturn(mockEndpointRef)
   ```

5. **Test Data Management**:
   - Use temporary directories for file-based tests
   - Clean up test data in `afterEach` or `afterAll`
   - Use `Utils.createTempDir()` for temp directories

### Pre-commit Checks

Before committing code, run:

```bash
# Format code
./dev/reformat

# Run affected tests
./build/mvn -pl <module> test

# Check style
./build/sbt scalastyle

# Check licenses
./build/mvn apache-rat:check

# Full pre-commit check (all of the above)
./build/mvn clean test
```

### Definition of Done for Tests

Before marking work complete:
- [ ] All new code has corresponding unit tests
- [ ] Edge cases are covered
- [ ] Error conditions are tested
- [ ] Integration tests pass (if applicable)
- [ ] No test failures in CI
- [ ] Code coverage maintained or improved

---

## 8. Useful Resources

### Documentation

**In the Repository** (`docs/` directory):
- **Overview**: `docs/developers/overview.md`
- **Master**: `docs/developers/master.md`
- **Worker**: `docs/developers/worker.md`
- **Client**: `docs/developers/client.md`
- **Configuration**: `docs/developers/configuration.md`
- **Monitoring**: `docs/monitoring.md`
- **Security**: `docs/security.md`
- **Deployment**: `docs/deploy.md`, `docs/deploy_on_k8s.md`
- **Migration**: `docs/migration.md`
- **REST API**: `docs/restapi.md`

**Online**:
- **Official Website**: https://celeborn.apache.org
- **Documentation**: https://celeborn.apache.org/docs/latest/
- **JIRA**: https://issues.apache.org/jira/projects/CELEBORN
- **GitHub**: https://github.com/apache/celeborn

### Community

**Communication Channels**:
- **Slack**: [Join Apache Celeborn Slack](https://join.slack.com/t/apacecelebor-kw08030/shared_invite/zt-1ju3hd5j8-4Z5keMdzpcVMspe4UJzF4Q)
- **Mailing Lists**:
  - Dev: dev@celeborn.apache.org ([Subscribe](mailto:dev-subscribe@celeborn.apache.org))
  - User: user@celeborn.apache.org ([Subscribe](mailto:user-subscribe@celeborn.apache.org))
- **GitHub Discussions**: https://github.com/apache/celeborn/discussions

**Getting Help**:
1. Check existing documentation
2. Search JIRA for similar issues
3. Ask on Slack or mailing lists
4. File a JIRA ticket if it's a bug or feature request

### Development Tools

**Ratis Shell** (for debugging HA state):
```bash
# Located in bin/celeborn-ratis-shell.sh
./bin/celeborn-ratis-shell.sh

# Example commands
> local
> peer list
> election pause
```
See `docs/developers/celeborn_ratis_shell.md` for details.

**Dependency Management**:
```bash
# List dependencies
./dev/dependencies.sh

# Check for updates
./build/mvn versions:display-dependency-updates
```

**Code Formatting**:
```bash
# Format all code
./dev/reformat

# Format web module only
./dev/reformat --web
```

**Merge Pull Requests** (for committers):
```bash
./dev/merge_pr.py
```

### Learning Path for New Contributors

1. **Week 1: Understand the Basics**
   - Read `docs/developers/overview.md`
   - Build the project
   - Run a local cluster (see `docs/deploy.md`)
   - Run a simple Spark job with Celeborn

2. **Week 2: Deep Dive into Architecture**
   - Study Master implementation (`master/src/main/scala/`)
   - Study Worker implementation (`worker/src/main/scala/`)
   - Review RPC framework (`common/src/main/scala/org/apache/celeborn/common/rpc/`)
   - Understand shuffle lifecycle

3. **Week 3: Make Your First Contribution**
   - Pick a "good first issue" from JIRA
   - Study related code
   - Write tests
   - Submit PR

4. **Week 4+: Advanced Topics**
   - High Availability (Ratis integration)
   - Multi-tier storage
   - Metrics and monitoring
   - Performance optimization

### Code Reading Tips

**Start Here**:
1. **`common/src/main/scala/org/apache/celeborn/common/CelebornConf.scala`**: Understand all configuration options
2. **`master/src/main/scala/org/apache/celeborn/service/deploy/master/Master.scala`**: Master lifecycle and slot allocation
3. **`worker/src/main/scala/org/apache/celeborn/service/deploy/worker/Worker.scala`**: Worker lifecycle and data handling
4. **`client/src/main/scala/org/apache/celeborn/client/LifecycleManager.scala`**: Client-side shuffle management
5. **`common/src/main/scala/org/apache/celeborn/common/protocol/message/ControlMessages.scala`**: RPC message definitions

**Code Navigation**:
- Use an IDE with Scala support (IntelliJ IDEA recommended)
- Use "Find Usages" to understand how APIs are used
- Follow the data flow from client → master → worker
- Read tests to understand expected behavior

### Performance Tuning Resources

**Key Configuration Areas**:
- Memory: `celeborn.worker.flusher.buffer.size`, `CELEBORN_WORKER_OFFHEAP_MEMORY`
- Network: `celeborn.network.io.numConnectionsPerPeer`, `celeborn.push.maxReqsInFlight`
- Storage: `celeborn.worker.flusher.threads`, storage tier configuration
- Compression: `celeborn.shuffle.compression.codec`

**Monitoring for Performance**:
- Watch `OfferSlotsTime`: Slot allocation latency
- Watch `FlushDataTime`: Flush performance by storage tier
- Watch `FetchChunkSuccessCount`: Read throughput
- Monitor JVM metrics: GC pauses, heap usage

### Reporting Issues

**Before Filing a Bug**:
1. Search existing JIRA tickets
2. Check if it's already fixed in main branch
3. Reproduce with minimal configuration
4. Gather logs and metrics

**JIRA Ticket Template**:
- **Summary**: Brief, specific description
- **Description**: Detailed explanation with steps to reproduce
- **Environment**: Celeborn version, Spark/Flink version, deployment environment
- **Logs**: Relevant log excerpts (use code blocks)
- **Expected vs Actual**: What should happen vs what actually happens

---

## Contributing

Thank you for your interest in contributing to Apache Celeborn! Your contributions make a real difference.

**Quick Start**:
1. Join the [Slack workspace](https://join.slack.com/t/apacecelebor-kw08030/shared_invite/zt-1ju3hd5j8-4Z5keMdzpcVMspe4UJzF4Q)
2. Check [good first issues](https://issues.apache.org/jira/issues/?jql=project%20%3D%20CELEBORN%20AND%20labels%20%3D%20%22good-first-issue%22)
3. Read `CONTRIBUTING.md` in the repository
4. Start coding!

**Code of Conduct**:
All contributors are expected to follow the [Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).

---

**Welcome to the Celeborn community! We look forward to your contributions!**
