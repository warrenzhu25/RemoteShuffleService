# Celeborn Master Component - Technical Deep Dive

## Table of Contents

1. [Initialization and Lifecycle](#1-initialization-and-lifecycle)
2. [Slot Allocation System](#2-slot-allocation-system)
3. [High Availability Implementation](#3-high-availability-implementation)
4. [Worker Management](#4-worker-management)
5. [Shuffle Metadata Management](#5-shuffle-metadata-management)
6. [Internal State Management](#6-internal-state-management)
7. [Message Handling](#7-message-handling)

---

## 1. Initialization and Lifecycle

### 1.1 Startup Sequence

**Location**: `master/src/main/scala/org/apache/celeborn/service/deploy/master/Master.scala`

The Master initialization follows a well-defined sequence:

#### Step 1: Configuration and Arguments Parsing
```scala
// Entry point: Master.main()
val conf = new CelebornConf()
val masterArgs = new MasterArguments(args, conf)
val master = new Master(conf, masterArgs)
```

#### Step 2: Metrics System Setup
```scala
// Initialize metrics system with multiple sources
val metricsSystem = MetricsSystem.createMetricsSystem(serviceName, conf)

// Register metric sources:
- ResourceConsumptionSource: Track resource usage
- MasterSource: Master-specific metrics
- ThreadPoolSource: Thread pool monitoring
- JVMSource: JVM metrics
- JVMCPUSource: CPU metrics
- SystemMiscSource: System miscellaneous metrics
```

#### Step 3: RPC Environment Setup

The Master creates two RPC environments:
- **External RPC Env** (port from args): Handles client and worker communication
- **Internal RPC Env** (optional, separate port): Used for HA master-to-master communication

```scala
// External RPC with optional authentication
val rpcEnv = if (!authEnabled) {
  RpcEnv.create(
    RpcNameConstants.MASTER_SYS,
    TransportModuleConstants.RPC_SERVICE_MODULE,
    masterArgs.host,
    masterArgs.port,
    conf,
    Math.max(64, Runtime.getRuntime.availableProcessors()),
    Role.MASTER,
    None, None)
} else {
  // With SASL authentication
  val externalSecurityContext = new RpcSecurityContextBuilder()
    .withServerSaslContext(new ServerSaslContextBuilder()
      .withAddRegistrationBootstrap(true)
      .withSecretRegistry(secretRegistry).build()).build()
  RpcEnv.create(..., Some(externalSecurityContext), None)
}
```

#### Step 4: Metadata Management System Initialization

```scala
val statusSystem = if (haEnabled) {
  val sys = new HAMasterMetaManager(internalRpcEnvInUse, conf, rackResolver)
  val handler = new MetaHandler(sys)
  handler.setUpMasterRatisServer(conf, masterArgs.masterClusterInfo.get)
  sys
} else {
  new SingleMasterMetaManager(internalRpcEnvInUse, conf, rackResolver)
}
```

#### Step 5: Background Tasks Scheduling
```scala
override def onStart(): Unit = {
  // Worker timeout checking
  checkForWorkerTimeoutTask = scheduleCheckTask(
    workerHeartbeatTimeoutMs, pbCheckForWorkerTimeout)

  // Application timeout checking
  checkForApplicationTimeOutTask = scheduleCheckTask(
    appHeartbeatTimeoutMs / 2, CheckForApplicationTimeOut)

  // Worker unavailability info expiration
  if (workerUnavailableInfoExpireTimeoutMs > 0) {
    checkForUnavailableWorkerTimeOutTask = scheduleCheckTask(
      workerUnavailableInfoExpireTimeoutMs / 2,
      CheckForWorkerUnavailableInfoTimeout)
  }

  // Partition size estimation updates
  partitionSizeUpdateService.scheduleWithFixedDelay(
    statusSystem.handleUpdatePartitionSize(),
    estimatedPartitionSizeUpdaterInitialDelay,
    estimatedPartitionSizeForEstimationUpdateInterval,
    TimeUnit.MILLISECONDS)
}
```

### 1.2 Thread Pools and Executors

| Thread Pool | Purpose | Configuration |
|------------|---------|---------------|
| `forwardMessageThread` | Scheduled tasks for timeout checks | Single thread, daemon |
| `nonEagerHandler` | Non-blocking application operations | Cached thread pool, 64 core threads |
| `sendApplicationMetaExecutor` | Push app metadata to workers | Fixed pool, configurable size |
| `partitionSizeUpdateService` | Partition size estimation | Single thread, scheduled |
| `quotaChecker` | Resource consumption tracking | Single thread, scheduled |

### 1.3 Shutdown and Cleanup Procedures

```scala
override def onStop(): Unit = {
  logInfo("Stopping Celeborn Master.")

  // Cancel all scheduled tasks
  Option(checkForWorkerTimeoutTask).foreach(_.cancel(true))
  Option(checkForUnavailableWorkerTimeOutTask).foreach(_.cancel(true))
  Option(checkForApplicationTimeOutTask).foreach(_.cancel(true))

  // Shutdown thread pools
  forwardMessageThread.shutdownNow()
  rackResolver.stop()

  // Stop metrics
  metricsSystem.stop()

  // Close Hadoop filesystems
  if (hadoopFs != null) {
    hadoopFs.asScala.foreach { case (storageType, fs) => fs.close() }
  }
}
```

---

## 2. Slot Allocation System

**Location**: `master/src/main/java/org/apache/celeborn/service/deploy/master/SlotsAllocator.java`

The Master implements two slot allocation strategies: **Round Robin** and **Load Aware**.

### 2.1 Core Concepts

**Slots**: A slot represents storage capacity on a worker for a single partition. Each shuffle partition needs:
- A primary slot (on one worker)
- A replica slot (on a different worker, if replication enabled)

**Partition Location**: Contains:
- Worker host and ports (RPC, push, fetch, replicate)
- Storage type and mount point
- Mode (PRIMARY or REPLICA)
- Peer reference (for primary-replica linkage)

### 2.2 Round Robin Allocation

**Algorithm Overview**:
```java
public static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
    offerSlotsRoundRobin(
        List<WorkerInfo> workers,
        List<Integer> partitionIds,
        boolean shouldReplicate,
        boolean shouldRackAware,
        int availableStorageTypes,
        boolean interruptionAware,
        int interruptionAwareThreshold)
```

**Key Steps**:

1. **Worker Validation**: Ensure at least 2 workers if replication is enabled
2. **Disk Selection**: Filter healthy disks by storage type
3. **Slot Allocation**: Round-robin distribution starting from random index

**Round Robin Logic**:
```java
// Start at random index
int primaryIndex = rand.nextInt(primaryWorkersSize);
int replicaIndex = rand.nextInt(replicaWorkersSize);

while (iter.hasPrevious()) {
  int partitionId = iter.previous();

  // Find primary worker with available slots
  while (!haveUsableSlots(slotsRestrictions, primaryWorkers, nextPrimaryInd)) {
    nextPrimaryInd = (nextPrimaryInd + 1) % primaryWorkersSize;
    if (nextPrimaryInd == primaryIndex) break outer;
  }

  // Create primary partition location
  PartitionLocation primaryPartition = createLocation(...);

  if (shouldReplicate) {
    // Find replica worker (different from primary, different rack if rack-aware)
    while ((nextReplicaInd == nextPrimaryInd && skipLocationsOnSameWorkerCheck)
        || !haveUsableSlots(slotsRestrictions, replicaWorkers, nextReplicaInd)
        || !satisfyRackAware(shouldRackAware, ...)) {
      nextReplicaInd = (nextReplicaInd + 1) % replicaWorkersSize;
      if (nextReplicaInd == replicaIndex) break outer;
    }

    // Create replica partition location
    PartitionLocation replicaPartition = createLocation(...);
    primaryPartition.setPeer(replicaPartition);
  }

  // Advance indices
  primaryIndex = (nextPrimaryInd + 1) % primaryWorkersSize;
  replicaIndex = (nextReplicaInd + 1) % replicaWorkersSize;
}
```

### 2.3 Load Aware Allocation

**Algorithm Overview**: Distributes partitions based on disk performance metrics, grouping disks by performance.

```java
public static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
    offerSlotsLoadAware(
        List<WorkerInfo> workers,
        List<Integer> partitionIds,
        boolean shouldReplicate,
        boolean shouldRackAware,
        int diskGroupCount,              // Number of performance groups
        double diskGroupGradient,        // Performance ratio between groups
        double flushTimeWeight,          // Weight for flush time
        double fetchTimeWeight,          // Weight for fetch time
        int availableStorageTypes,
        boolean interruptionAware,
        int interruptionAwareThreshold)
```

**Key Steps**:

1. **Disk Performance Scoring**:
```java
usableDisks.sort((o1, o2) -> {
  double delta =
    (o1.avgFlushTime() * flushTimeWeight + o1.avgFetchTime() * fetchTimeWeight)
    - (o2.avgFlushTime() * flushTimeWeight + o2.avgFetchTime() * fetchTimeWeight);
  return delta < 0 ? -1 : (delta > 0 ? 1 : 0);
});
```

2. **Disk Grouping**: Divide disks into performance groups

3. **Allocation Ratio Calculation**:
```java
// Initialize allocation ratios using geometric series
taskAllocationRatio = new double[diskGroups];
double totalAllocations = 0;
for (int i = 0; i < diskGroups; i++) {
  totalAllocations += Math.pow(1 + diskGroupGradient, diskGroups - 1 - i);
}
for (int i = 0; i < diskGroups; i++) {
  taskAllocationRatio[i] =
    Math.pow(1 + diskGroupGradient, diskGroups - 1 - i) / totalAllocations;
}
```

**Example**: With 3 disk groups and gradient 0.1:
- Group 0 (fastest): 36.7% of partitions
- Group 1 (medium): 33.3% of partitions
- Group 2 (slowest): 30.0% of partitions

### 2.4 Rack-Aware Selection

```java
static List<WorkerInfo> generateRackAwareWorkers(List<WorkerInfo> workers) {
  // Group workers by rack
  Map<String, LinkedList<WorkerInfo>> rackToHosts;
  for (WorkerInfo worker : workers) {
    map.computeIfAbsent(worker.networkLocation(), key -> new LinkedList<>())
       .add(worker);
  }

  // Sort racks by number of hosts (descending)
  sortedRackToHosts.sort((o1, o2) ->
    Integer.compare(o2.getValue().size(), o1.getValue().size()));

  // Round-robin across racks to distribute hosts evenly
  while (count < numWorkers) {
    for (LinkedList<WorkerInfo> workerList : sortedRackToHosts) {
      result.add(workerList.removeFirst());
      count++;
      if (workerList.isEmpty()) remove rack;
    }
  }
  return result;
}
```

### 2.5 Slot Reservation Process

```scala
def handleRequestSlots(context: RpcCallContext, requestSlots: RequestSlots): Unit = {
  val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)

  // 1. Get available workers (excluding blacklisted)
  var availableWorkers = workersAvailable(requestSlots.excludedWorkerSet)

  // 2. Apply tag-based filtering
  if (conf.tagsEnabled) {
    availableWorkers = tagsManager.getTaggedWorkers(
      requestSlots.userIdentifier,
      requestSlots.tagsExpr,
      availableWorkers)
  }

  // 3. Calculate number of workers needed
  val numWorkers = Math.min(
    Math.max(
      if (requestSlots.shouldReplicate) 2 else 1,
      if (requestSlots.maxWorkers <= 0) slotsAssignMaxWorkers
      else Math.min(slotsAssignMaxWorkers, requestSlots.maxWorkers)),
    numAvailableWorkers)

  // 4. Select workers randomly
  val startIndex = Random.nextInt(numAvailableWorkers)
  val selectedWorkers = new util.ArrayList[WorkerInfo](numWorkers)

  // 5. Allocate slots using selected policy
  val slots = statusSystem.workersMap.synchronized {
    if (slotsAssignPolicy == SlotsAssignPolicy.LOADAWARE) {
      SlotsAllocator.offerSlotsLoadAware(...)
    } else {
      SlotsAllocator.offerSlotsRoundRobin(...)
    }
  }

  // 6. Update metadata
  statusSystem.handleRequestSlots(
    shuffleKey, hostname, slotsPerDisk, requestId)

  // 7. Reply to client
  context.reply(RequestSlotsResponse(StatusCode.SUCCESS, slots, packed))
}
```

### 2.6 Worker Exclusion Management

**Worker Exclusion Types**:
1. **Automatic Exclusion** (`excludedWorkers`): Workers with no available storage or high workload
2. **Manual Exclusion** (`manuallyExcludedWorkers`): Operators explicitly exclude via API
3. **Lost Workers** (`lostWorkers`): Workers that failed heartbeat timeout
4. **Shutdown Workers** (`shutdownWorkers`): Workers gracefully shutting down
5. **Decommission Workers** (`decommissionWorkers`): Workers being decommissioned

---

## 3. High Availability Implementation

Celeborn Master HA is built on **Apache Ratis**, a Java implementation of the Raft consensus protocol.

### 3.1 Raft Consensus Integration

**Architecture**:
```
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│   Master 1      │         │   Master 2      │         │   Master 3      │
│  (Leader)       │◄───────►│  (Follower)     │◄───────►│  (Follower)     │
│  HARaftServer   │         │  HARaftServer   │         │  HARaftServer   │
│  StateMachine   │         │  StateMachine   │         │  StateMachine   │
└─────────────────┘         └─────────────────┘         └─────────────────┘
         │                           │                           │
         └───────────────────────────┴───────────────────────────┘
                          Raft Log Replication
```

### 3.2 Leader Election Process

**Election Configuration**:
```java
// Default timeouts
rpcTimeoutMin: 3s
rpcTimeoutMax: 5s
firstElectionTimeoutMin: 10s
firstElectionTimeoutMax: 12s
```

**Leader Status Checking**:
```java
// Check every roleCheckIntervalMs (default: 5s)
this.scheduledRoleChecker.scheduleWithFixedDelay(() -> {
  if (cachedPeerRole.isPresent() &&
      cachedPeerRole.get() == RaftProtos.RaftPeerRole.LEADER) {
    updateServerRole();
  }
}, roleCheckIntervalMs, roleCheckIntervalMs, TimeUnit.MILLISECONDS);
```

**Request Routing**:
```scala
// In Master.executeWithLeaderChecker
def executeWithLeaderChecker[T](context: RpcCallContext, f: => T): Unit = {
  if (HAHelper.checkShouldProcess(context, statusSystem, bindPreferIP)) {
    try {
      f  // Execute on leader
    } catch {
      case e: Exception =>
        HAHelper.sendFailure(sendFailureFn, ratisServer, e, bindPreferIP)
    }
  }
  // If not leader, HAHelper.checkShouldProcess sends MasterNotLeaderException
}
```

### 3.3 State Machine Replication

**Write Request Flow**:
```
Client Request
      │
      ▼
HAMasterMetaManager.handleXXX()
      │
      ▼
HARaftServer.submitRequest(ResourceRequest)
      │
      ▼
RaftServer.submitClientRequestAsync()
      │
      ▼
Raft Log Entry (replicated to followers)
      │
      ▼
StateMachine.applyTransaction(TransactionContext)
      │
      ▼
MetaHandler.handleWriteRequest(PbMetaRequest)
      │
      ▼
Response to Client
```

### 3.4 Snapshot Management

**Snapshot Configuration**:
```java
snapshotAutoTriggerEnabled: true
snapshotAutoTriggerThreshold: 200,000 log entries
snapshotRetentionFileNum: 3
```

**Snapshot Content**:
- Estimated partition size
- Registered applications and shuffles
- Worker information and status
- Excluded workers
- Application heartbeat times
- Statistics (totals, counts)

---

## 4. Worker Management

### 4.1 Worker Registration Process

```scala
def handleRegisterWorker(...): Unit = {
  val workerToRegister = new WorkerInfo(
    host, rpcPort, pushPort, fetchPort, replicatePort, internalPort,
    disks, userResourceConsumption)

  // 1. Check if worker host is allowed
  if (!workerHostAllowedToRegister(host)) {
    context.reply(RegisterWorkerResponse(false, "Host pattern mismatch"))
    return
  }

  // 2. Handle re-registration or new registration
  if (statusSystem.workersMap.containsKey(workerToRegister.toUniqueId)) {
    logWarning("Worker already exists, re-register")
    statusSystem.handleRegisterWorker(...)
  } else {
    statusSystem.handleRegisterWorker(...)
    logInfo(s"Registered worker $workerToRegister")
  }

  context.reply(RegisterWorkerResponse(true, ""))
}
```

### 4.2 Heartbeat Handling

**Heartbeat Processing**:
```scala
private def handleHeartbeatFromWorker(...): Unit = {
  val targetWorker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort)
  val registered = statusSystem.workersMap.containsKey(targetWorker.toUniqueId)

  if (registered) {
    // Update worker state
    statusSystem.handleWorkerHeartbeat(...)
  }

  // Find expired shuffle keys
  val expiredShuffleKeys = detectExpiredShuffles(activeShuffleKeys)

  context.reply(HeartbeatFromWorkerResponse(expiredShuffleKeys, registered))
}
```

**Timeout Detection**:
```scala
private def timeoutDeadWorkers(): Unit = {
  val currentTime = System.currentTimeMillis()

  // Avoid timeouts during leader election
  if (HAHelper.getWorkerTimeoutDeadline(statusSystem) > currentTime) {
    return
  }

  statusSystem.workersMap.values().asScala.foreach { worker =>
    if (worker.lastHeartbeat < currentTime - workerHeartbeatTimeoutMs) {
      logWarning(s"Worker ${worker.readableAddress()} timeout!")
      self.send(WorkerLost(...))
    }
  }
}
```

### 4.3 Worker Status Tracking

**Worker States**:
- Normal
- Graceful_Shutdown
- Decommissioning

**WorkerInfo Class**:
```scala
class WorkerInfo(
  val host: String,
  val rpcPort: Int,
  val pushPort: Int,
  val fetchPort: Int,
  val replicatePort: Int,
  val internalPort: Int,
  private val diskInfos: java.util.Map[String, DiskInfo],
  private val userResourceConsumption: java.util.Map[UserIdentifier, ResourceConsumption]) {

  @volatile var lastHeartbeat: Long = 0
  @volatile var networkLocation: String = NetworkTopology.DEFAULT_RACK
  @volatile var workerStatus: WorkerStatus = WorkerStatus.normalWorkerStatus()
  @volatile var isHighWorkLoad: Boolean = false

  def totalAvailableSlots(): Long =
    diskInfos.values.asScala.map(_.getAvailableSlots).sum
}
```

### 4.4 Disk Slot Calculation

```scala
def updateDiskSlots(estimatedPartitionSize: Long): Unit = {
  diskInfos.values.asScala.foreach { disk =>
    if (disk.status == DiskStatus.HEALTHY) {
      val availableSlots = disk.actualUsableSpace / estimatedPartitionSize
      disk.setAvailableSlots(availableSlots)
    } else {
      disk.setAvailableSlots(0)
    }
  }
}
```

---

## 5. Shuffle Metadata Management

### 5.1 Shuffle Registration

```scala
// Triggered by RequestSlots
statusSystem.handleRequestSlots(
  shuffleKey,
  hostname,
  workerToAllocatedSlots,
  requestId)

// Data Structures:
// registeredAppAndShuffles: appId -> Set[shuffleId]
// appHeartbeatTime: appId -> timestamp
// hostnameSet: Set[hostname]
```

### 5.2 Application Lifecycle Tracking

**Application Heartbeat**:
```scala
def handleHeartbeatFromApplication(...): Unit = {
  // Update heartbeat time
  statusSystem.updateAppHeartbeatMeta(appId, time, ...)

  // Return excluded workers, unknown workers, quota status
  context.reply(HeartbeatFromApplicationResponse(...))
}
```

**Application Timeout Detection**:
```scala
private def timeoutDeadApplications(): Unit = {
  val currentTime = System.currentTimeMillis()

  statusSystem.appHeartbeatTime.asScala.foreach { case (appId, heartbeatTime) =>
    if (heartbeatTime < currentTime - appHeartbeatTimeoutMs) {
      logWarning(s"Application $appId timeout")
      handleApplicationLost(null, appId, requestId)
    }
  }
}
```

### 5.3 Partition Size Estimation

```java
public void updatePartitionSize() {
  long tmpTotalWritten = partitionTotalWritten.sumThenReset();
  long tmpFileCount = partitionTotalFileCount.sumThenReset();

  if (tmpFileCount != 0) {
    estimatedPartitionSize = Math.max(
      conf.minPartitionSizeToEstimate(),
      Math.min(tmpTotalWritten / tmpFileCount, conf.maxPartitionSizeToEstimate()));
  } else {
    estimatedPartitionSize = initialEstimatedPartitionSize;
  }

  // Update worker disk slots if size changed
  if (estimatedPartitionSize != oldEstimatedPartitionSize) {
    workers.forEach(workerInfo -> workerInfo.updateDiskSlots(estimatedPartitionSize))
  }
}
```

**Configuration**:
- Initial: 64MB (default)
- Min: 8MB (default)
- Max: 1GB (default)
- Update interval: 10 minutes (default)

---

## 6. Internal State Management

### 6.1 Data Structures for Workers

```java
// Primary worker registry
public final Map<String, WorkerInfo> workersMap = JavaUtils.newConcurrentHashMap();

// Available workers for slot allocation
public final Set<WorkerInfo> availableWorkers = ConcurrentHashMap.newKeySet();

// Lost workers: WorkerInfo -> lostTime
public final ConcurrentHashMap<WorkerInfo, Long> lostWorkers =
  JavaUtils.newConcurrentHashMap();

// Excluded workers (automatic)
public final Set<WorkerInfo> excludedWorkers = ConcurrentHashMap.newKeySet();

// Manually excluded workers (persisted)
public final Set<WorkerInfo> manuallyExcludedWorkers = ConcurrentHashMap.newKeySet();

// Shutdown workers
public final Set<WorkerInfo> shutdownWorkers = ConcurrentHashMap.newKeySet();

// Decommission workers
public final Set<WorkerInfo> decommissionWorkers = ConcurrentHashMap.newKeySet();
```

### 6.2 Data Structures for Shuffle Metadata

```java
// Shuffle registration
public final Map<String, Set<Integer>> registeredAppAndShuffles =
  JavaUtils.newConcurrentHashMap();

// Application info
public final ConcurrentHashMap<String, ApplicationInfo> applicationInfos =
  JavaUtils.newConcurrentHashMap();

// Application heartbeats
public final ConcurrentHashMap<String, Long> appHeartbeatTime =
  JavaUtils.newConcurrentHashMap();

// Statistics
public final LongAdder partitionTotalWritten = new LongAdder();
public final LongAdder partitionTotalFileCount = new LongAdder();
public final LongAdder shuffleTotalCount = new LongAdder();
public final LongAdder applicationTotalCount = new LongAdder();
```

### 6.3 Synchronization and Concurrency Control

**Critical Sections**:

```scala
// Worker Registration
synchronized (workersMap) {
  workersMap.putIfAbsent(workerInfo.toUniqueId(), workerInfo)
  shutdownWorkers.remove(workerInfo)
  lostWorkers.remove(workerInfo)
  excludedWorkers.remove(workerInfo)
  updateAvailableWorkers(workerInfo)
}

// Slot Allocation
val slots = statusSystem.workersMap.synchronized {
  if (slotsAssignPolicy == SlotsAssignPolicy.LOADAWARE) {
    SlotsAllocator.offerSlotsLoadAware(...)
  } else {
    SlotsAllocator.offerSlotsRoundRobin(...)
  }
}
```

---

## 7. Message Handling

### 7.1 Key RPC Messages

**From Applications**:
- `RegisterApplicationInfo`: Application registration
- `HeartbeatFromApplication`: Application heartbeat
- `RequestSlots`: Request shuffle slots
- `UnregisterShuffle`: Unregister shuffle
- `CheckQuota`: Check resource quota

**From Workers**:
- `RegisterWorker`: Worker registration
- `HeartbeatFromWorker`: Worker heartbeat
- `ReportWorkerUnavailable`: Report unavailable workers
- `WorkerLost`: Worker lost notification

**Administrative Operations**:
- `WorkerExclude`: Manual worker exclusion
- `WorkerEventRequest`: Worker lifecycle events
- `ReviseLostShuffles`: Revise lost shuffles

### 7.2 Request/Response Flows

**Slot Allocation Flow**:
```
Client → RequestSlots → Master (Leader)
  1. Check authentication
  2. Get available workers
  3. Apply tag filtering
  4. Select workers
  5. Allocate slots
  6. Update metadata (via Raft if HA)
  7. Push app metadata to workers
Master → RequestSlotsResponse → Client
```

**HA Write Request Flow**:
```
Client → RPC Request → Master Follower
  → MasterNotLeaderException (with leader endpoints)
Client → Retry to Leader → Master Leader
  1. Convert to ResourceRequest
  2. Submit to Raft
  3. Log Replication to Followers
  4. Ack (majority)
  5. Apply to StateMachine
  6. Return response
Master → Response → Client
```

### 7.3 Error Handling

**Error Types**:
1. **Worker Not Registered**: Return `registered=false` in heartbeat response
2. **Slot Allocation Failure**: Return `SLOT_NOT_AVAILABLE` status
3. **Authentication Failure**: Send failure with `CelebornException`
4. **Master Not Leader**: Throw `MasterNotLeaderException` with leader endpoints

---

## Summary

The Celeborn Master is a sophisticated distributed coordination service that:

1. **Manages Cluster Resources**: Worker registration, health monitoring, capacity tracking
2. **Allocates Shuffle Slots**: Round Robin or Load Aware strategies with rack-awareness
3. **Provides High Availability**: Raft consensus for strong consistency and automatic failover
4. **Tracks Shuffle Metadata**: Application lifecycle, partition size estimation, cleanup
5. **Ensures Data Safety**: State machine replication, snapshot management, graceful degradation

The design emphasizes:
- **Strong Consistency**: Via Raft consensus
- **Intelligent Resource Management**: Load-aware allocation and dynamic adaptation
- **High Availability**: Multi-master with automatic failover
- **Scalability**: Efficient data structures and concurrent operations
