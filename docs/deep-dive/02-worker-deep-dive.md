# Celeborn Worker Component - Technical Deep Dive

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Storage System](#2-storage-system)
3. [Data Push Flow](#3-data-push-flow)
4. [Data Fetch Flow](#4-data-fetch-flow)
5. [Flusher System](#5-flusher-system)
6. [Device Monitoring](#6-device-monitoring)
7. [Partition Management](#7-partition-management)
8. [Traffic Control](#8-traffic-control)

---

## 1. Architecture Overview

### 1.1 The 4 Servers Architecture

**Location**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/Worker.scala`

```
┌─────────────────────────────────────────────────────────┐
│                     Worker Process                       │
├─────────────────────────────────────────────────────────┤
│  Controller (RPC)   │  Control messages                 │
│  Push Server        │  Receive shuffle data from mappers│
│  Replicate Server   │  Receive replica data from peers  │
│  Fetch Server       │  Serve data to reducers           │
├─────────────────────────────────────────────────────────┤
│  StorageManager     │  Multi-tier storage management    │
│  Flusher System     │  Async data flushing              │
│  DeviceMonitor      │  Disk health monitoring           │
│  MemoryManager      │  Memory pressure handling         │
└─────────────────────────────────────────────────────────┘
```

**Push Server** (port: `celeborn.worker.pushPort`):
- **Purpose**: Receives shuffle data from executors
- **IO Threads**: Configurable, defaults to `totalFlusherThread`
- **Handler**: `PushDataHandler`
- **Features**: Heartbeat enabled, channels limiter for backpressure

**Replicate Server** (port: `celeborn.worker.replicatePort`):
- **Purpose**: Receives replicated data from peer workers
- **Handler**: Separate `PushDataHandler` instance
- **Features**: No heartbeat (internal replication)

**Fetch Server** (port: `celeborn.worker.fetchPort`):
- **Purpose**: Serves shuffle data to reducers
- **Handler**: `FetchHandler`
- **Features**: Chunk streaming and credit-based flow control

**Controller** (RPC):
- **Purpose**: Handles control messages (ReserveSlots, CommitFiles, Destroy)
- **Handler**: `Controller` RPC endpoint

### 1.2 Thread Pools

```scala
// Data replication
replicateThreadPool = ThreadUtils.newDaemonCachedThreadPool(
  "worker-data-replicator",
  conf.workerReplicateThreads)

// File commit operations
commitThreadPool = ThreadUtils.newDaemonCachedThreadPool(
  "worker-files-committer",
  conf.workerCommitThreads)

// Cleanup operations
cleanThreadPool = ThreadUtils.newDaemonCachedThreadPool(
  "worker-expired-shuffle-cleaner",
  conf.workerCleanThreads)
```

---

## 2. Storage System

### 2.1 Multi-Tier Storage Architecture

**Location**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/StorageManager.scala`

```
Memory Buffer (fastest)
     ↓
Local Disk (SSD/HDD)
     ↓
HDFS (distributed)
     ↓
Object Storage (S3/OSS)
```

### 2.2 File Creation Logic

```scala
def createFile(partitionDataWriterContext: PartitionDataWriterContext,
    useMemoryShuffle: Boolean): (MemoryFileInfo, Flusher, DiskFileInfo, File) = {

  val location = partitionDataWriterContext.getPartitionLocation

  // Tier 1: Try Memory
  if (useMemoryShuffle
      && location.getStorageInfo.memoryAvailable()
      && MemoryManager.instance().memoryFileStorageAvailable()) {
    return (createMemoryFileInfo(...), null, null, null)
  }

  // Tier 2-4: Try Disk/HDFS/S3/OSS
  else if (location.getStorageInfo.localDiskAvailable()
      || location.getStorageInfo.HDFSAvailable()
      || location.getStorageInfo.S3Available()
      || location.getStorageInfo.OSSAvailable()) {
    val result = createDiskFile(...)
    return (null, result._1, result._2, result._3)
  }

  return (null, null, null, null)
}
```

### 2.3 Directory Structure

```
<mount-point>/
  <working-dir>/           # e.g., "celeborn-worker"
    <app-id>/             # e.g., "app-20231101-0001"
      <shuffle-id>/       # e.g., "1"
        <partition-file>  # e.g., "0-1-0-0-1-primary"
```

**File Naming**: `<partitionId>-<epoch>-<attemptId>-<index>-<mode>`

---

## 3. Data Push Flow

### 3.1 PushData Handling

**Location**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/PushDataHandler.scala`

```scala
override def receive(client: TransportClient, msg: RequestMessage): Unit = msg match {
  case pushData: PushData =>
    workerSource.recordAppActiveConnection(client, pushData.shuffleKey)
    val callback = new SimpleRpcResponseCallback(client, pushData.requestId, pushData.shuffleKey)

    handleCore(client, pushData, pushData.requestId, pushData.shuffleKey,
      () => {
        val partitionType = shufflePartitionType.getOrDefault(pushData.shuffleKey, PartitionType.REDUCE)
        partitionType match {
          case PartitionType.REDUCE => handlePushData(pushData, callback)
          case PartitionType.MAP => handleMapPartitionPushData(pushData, callback)
        }
      },
      callback)
}
```

### 3.2 Replication Flow

**Primary-Replica Pattern**:

```
Mapper → Primary Worker
  │          ↓
  │     Write to disk
  │          ↓
  │     Async replicate → Replica Worker
  │                          ↓
  │                     Write to disk
  │          ↓
  │     Both succeed
  │          ↓
  ←─── Success response
```

**Replication Thread**:
```scala
replicateThreadPool.submit(new Runnable {
  override def run(): Unit = {
    val peerWorker = location.getPeer
    val client = getReplicateClient(peer.getHost, peer.getReplicatePort, location.getId)

    val newPushData = new PushData(
      PartitionLocation.Mode.REPLICA.mode(),
      shuffleKey,
      pushData.partitionUniqueId,
      pushData.body)

    client.pushData(newPushData, shufflePushDataTimeout.get(shuffleKey), wrappedCallback)
  }
})
```

### 3.3 Partition Split Detection

```scala
private def checkDiskFullAndSplit(fileWriter: PartitionDataWriter, isPrimary: Boolean): StatusCode = {
  // Check memory shuffle storage limit
  if (fileWriter.needHardSplitForMemoryShuffleStorage()) {
    return StatusCode.HARD_SPLIT
  }

  val diskFull = checkDiskFull(fileWriter)

  if (workerPartitionSplitEnabled &&
      ((diskFull && fileLength > partitionSplitMinimumSize) ||
       (isPrimary && fileLength > fileWriter.getSplitThreshold))) {

    if (fileWriter.getSplitMode == PartitionSplitMode.SOFT &&
        fileLength < partitionSplitMaximumSize) {
      return StatusCode.SOFT_SPLIT
    } else {
      return StatusCode.HARD_SPLIT
    }
  }

  StatusCode.NO_SPLIT
}
```

**Soft Split**: Client can continue pushing while requesting new partition
**Hard Split**: Client must stop immediately and request new partition

---

## 4. Data Fetch Flow

### 4.1 Stream Management

**Location**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/FetchHandler.scala`

```scala
private def handleOpenStreamInternal(...): Unit = {
  val fileInfo = getRawFileInfo(shuffleKey, fileName)

  fileInfo.getFileMeta match {
    case _: ReduceFileMeta =>
      // Chunk-based streaming
      val pbStreamHandlerOpt = handleReduceOpenStreamInternal(...)
      replyStreamHandler(client, rpcRequestId, pbStreamHandlerOpt.getStreamHandler, isLegacy)

    case _: MapFileMeta =>
      // Credit-based streaming
      creditStreamManager.registerStream(
        creditStreamHandler,
        client.getChannel,
        shuffleKey,
        initialCredit,
        startIndex,
        endIndex,
        fileInfo.asInstanceOf[DiskFileInfo])
  }
}
```

### 4.2 Chunk Fetch Handling

```scala
def handleChunkFetchRequest(client: TransportClient, streamChunkSlice: StreamChunkSlice, req: RequestMessage): Unit = {
  val streamState = chunkStreamManager.getStreamState(streamChunkSlice.streamId)

  // Get chunk from file
  val buf = chunkStreamManager.getChunk(
    streamChunkSlice.streamId,
    streamChunkSlice.chunkIndex,
    streamChunkSlice.offset,
    streamChunkSlice.len)

  chunkStreamManager.chunkBeingSent(streamChunkSlice.streamId)

  client.getChannel.writeAndFlush(new ChunkFetchSuccess(streamChunkSlice, buf))
    .addListener(new GenericFutureListener[Future[_ >: Void]] {
      override def operationComplete(future: Future[_ >: Void]): Unit = {
        if (future.isSuccess) {
          workerSource.incCounter(WorkerSource.FETCH_CHUNK_SUCCESS_COUNT)
        }
        chunkStreamManager.chunkSent(streamChunkSlice.streamId)
      }
    })
}
```

### 4.3 Credit System

**Credit Flow**:
1. Client opens stream with initial credit
2. Worker sends chunks up to credit limit
3. Client sends `ReadAddCredit` to request more data
4. Worker updates credit and resumes sending

```scala
def handleReadAddCredit(client: TransportClient, credit: Int, streamId: Long, requestId: Long): Unit = {
  val shuffleKey = creditStreamManager.getStreamShuffleKey(streamId)
  if (shuffleKey != null) {
    creditStreamManager.addCredit(credit, streamId)
  }
}
```

---

## 5. Flusher System

### 5.1 Flusher Architecture

**Location**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/Flusher.scala`

```scala
abstract class Flusher(
    val workerSource: AbstractSource,
    val threadCount: Int,
    val allocator: ByteBufAllocator,
    val maxComponents: Int,
    flushTimeMetric: TimeWindow,
    mountPoint: String,
    val reuseCopyBuffer: Boolean,
    val maxTaskSize: Long) {

  protected val workingQueues = new Array[LinkedBlockingQueue[FlushTask]](threadCount)
  protected val workers = new Array[ExecutorService](threadCount)
}
```

### 5.2 Flusher Types

**LocalFlusher**: One per mount point
- Writes to local disk
- Monitors disk health via DeviceMonitor
- Reports errors to DeviceObserver

**HdfsFlusher**: Single instance
- Writes to HDFS via Hadoop FileSystem API
- Configurable thread count

**S3Flusher / OssFlusher**: For cloud storage

### 5.3 Flush Scheduling

```scala
def addTask(task: FlushTask, timeoutMs: Long, workerIndex: Int): Boolean = {
  workingQueues(workerIndex).offer(task, timeoutMs, TimeUnit.MILLISECONDS)
}

def getWorkerIndex: Int = synchronized {
  nextWorkerIndex = (nextWorkerIndex + 1) % threadCount
  nextWorkerIndex
}
```

---

## 6. Device Monitoring

### 6.1 DeviceMonitor Implementation

**Location**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/DeviceMonitor.scala`

```scala
class LocalDeviceMonitor(
    conf: CelebornConf,
    observer: DeviceObserver,
    deviceInfos: util.Map[String, DeviceInfo],
    diskInfos: util.Map[String, DiskInfo],
    workerSource: AbstractSource) extends DeviceMonitor {

  val diskCheckInterval = conf.workerDiskMonitorCheckInterval
  val deviceMonitorCheckList = conf.workerDiskMonitorCheckList
}
```

### 6.2 Health Checks

**Periodic Check**:
```scala
override def startCheck(): Unit = {
  diskChecker.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        observedDevices.values().asScala.foreach(device => {
          val mountPoints = device.diskInfos.keySet.asScala.toList

          // Check accumulated errors
          val nonCriticalErrorSum = device.nonCriticalErrors.values().asScala.map(_.size).sum
          if (nonCriticalErrorSum > device.notifyErrorThreshold) {
            device.notifyObserversOnError(mountPoints, DiskStatus.CRITICAL_ERROR)
          } else {
            // Individual disk checks
            if (checkIoHang && device.ioHang()) {
              device.notifyObserversOnNonCriticalError(mountPoints, DiskStatus.IO_HANG)
            } else {
              device.diskInfos.values().asScala.foreach { diskInfo =>
                if (checkDiskUsage && DeviceMonitor.highDiskUsage(conf, diskInfo)) {
                  device.notifyObserversOnHighDiskUsage(diskInfo.mountPoint)
                } else if (checkReadWrite && DeviceMonitor.readWriteError(conf, diskInfo.dirs.head)) {
                  device.notifyObserversOnNonCriticalError(List(diskInfo.mountPoint), DiskStatus.READ_OR_WRITE_FAILURE)
                } else if (nonCriticalErrorSum <= device.notifyErrorThreshold * 0.5) {
                  device.notifyObserversOnHealthy(diskInfo.mountPoint)
                }
              }
            }
          }
        })
      }
    },
    0,
    diskCheckInterval,
    TimeUnit.MILLISECONDS)
}
```

### 6.3 Disk Usage Check

```scala
def highDiskUsage(conf: CelebornConf, diskInfo: DiskInfo): Boolean = {
  val usage = getDiskUsageInfos(diskInfo)
  val actualReserveSize = DiskUtils.getActualReserveSize(
    diskInfo,
    conf.workerDiskReserveSize,
    conf.workerDiskReserveRatio)

  val highDiskUsage = usage.freeSpace < actualReserveSize || diskInfo.actualUsableSpace <= 0

  if (highDiskUsage) {
    logWarning(s"${diskInfo.mountPoint} usage above threshold")
  }
  highDiskUsage
}
```

---

## 7. Partition Management

### 7.1 Reserve Slots

**Location**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/Controller.scala`

```scala
private def handleReserveSlots(...): Unit = {
  // Check worker status
  if (shutdown.get()) {
    context.reply(ReserveSlotsResponse(StatusCode.WORKER_SHUTDOWN, "Worker shutting down"))
    return
  }

  // Create primary locations
  val primaryLocs = new jArrayList[PartitionLocation]()
  for (ind <- 0 until requestPrimaryLocs.size()) {
    var location = partitionLocationInfo.getPrimaryLocation(shuffleKey, requestPrimaryLocs.get(ind).getUniqueId)
    if (location == null) {
      location = requestPrimaryLocs.get(ind)
      val writer = storageManager.createPartitionDataWriter(...)
      primaryLocs.add(new WorkingPartition(location, writer))
    }
  }

  // Update partition info
  partitionLocationInfo.addPrimaryPartitions(shuffleKey, primaryLocs)
  workerInfo.allocateSlots(shuffleKey, Utils.getSlotsPerDisk(requestPrimaryLocs, requestReplicaLocs))

  context.reply(ReserveSlotsResponse(StatusCode.SUCCESS))
}
```

### 7.2 Commit Files

```scala
private def handleCommitFiles(...): Unit = {
  // Check if already committed
  shuffleCommitInfos.putIfAbsent(shuffleKey, JavaUtils.newConcurrentHashMap[Long, CommitInfo]())
  val commitInfo = epochCommitMap.get(epoch)

  commitInfo.synchronized {
    if (commitInfo.status == CommitInfo.COMMIT_FINISHED) {
      context.reply(commitInfo.response)
      return
    }
    commitInfo.status = CommitInfo.COMMIT_INPROCESS
  }

  // Commit files in thread pool
  val (primaryFuture, primaryTasks) = commitFiles(shuffleKey, primaryIds, ...)
  val (replicaFuture, replicaTasks) = commitFiles(shuffleKey, replicaIds, ..., isPrimary = false)

  val future = CompletableFuture.allOf(primaryFuture, replicaFuture)

  future.handleAsync(new BiFunction[Void, Throwable, Unit] {
    override def apply(v: Void, t: Throwable): Unit = {
      if (t != null) {
        commitInfo.response = CommitFilesResponse(StatusCode.COMMIT_FILE_EXCEPTION, ...)
      } else {
        // Prepare and send response
      }
      commitInfo.status = CommitInfo.COMMIT_FINISHED
    }
  }, asyncReplyPool)
}
```

### 7.3 Cleanup

```scala
def cleanup(expiredShuffleKeys: JHashSet[String], threadPool: ThreadPoolExecutor): Unit = synchronized {
  expiredShuffleKeys.asScala.foreach { shuffleKey =>
    // Clean partition locations
    partitionLocationInfo.removeShuffle(shuffleKey)
    shufflePartitionType.remove(shuffleKey)
    shufflePushDataTimeout.remove(shuffleKey)

    // Release slots
    workerInfo.releaseSlots(shuffleKey)
  }

  // Clean storage (async)
  threadPool.execute(new Runnable {
    override def run(): Unit = {
      storageManager.cleanupExpiredShuffleKey(expiredShuffleKeys)
    }
  })
}
```

---

## 8. Traffic Control

### 8.1 Memory-Based Backpressure

**Location**: `common/src/main/java/org/apache/celeborn/common/memory/MemoryManager.java`

```java
public void checkDirectMemoryPressure() {
  long memoryUsed = getNettyUsedDirectMemory();
  long memoryTarget = getMemoryTarget();

  if (memoryUsed > memoryTarget * PAUSE_PUSH_DATA_AND_REPLICATE_RATIO) {
    pausePushDataAndReplicate();
  } else if (memoryUsed > memoryTarget * PAUSE_PUSH_DATA_RATIO) {
    pausePushData();
  } else if (memoryUsed < memoryTarget * RESUME_RATIO) {
    resumePushAndReplicate();
  }
}
```

### 8.2 Congestion Control

**CongestionController**:
```java
public class CongestionController {
  private final TimeSlidingHub timeSlidingHub;
  private final BufferStatusHub bufferStatusHub;
  private final long highWatermark;
  private final long lowWatermark;

  public boolean isUserCongested(UserCongestionControlContext context) {
    UserBufferInfo userBufferInfo = bufferStatusHub.getUserBuffer(context);
    return userBufferInfo != null && userBufferInfo.isBackpressure();
  }

  public boolean isOverHighWatermark() {
    return timeSlidingHub.getTotalBytesInWindow() > highWatermark;
  }
}
```

### 8.3 High Workload Detection

```scala
def highWorkload: Boolean = {
  (memoryManager.currentServingState,
   Option(CongestionController.instance()),
   conf.workerActiveConnectionMax) match {
    case (_, Some(instance), _) if instance.isOverHighWatermark => true
    case (ServingState.PUSH_AND_REPLICATE_PAUSED, _, _) => true
    case (ServingState.PUSH_PAUSED, _, _) => true
    case (_, _, Some(activeConnectionMax)) =>
      workerSource.getCounterCount(WorkerSource.ACTIVE_CONNECTION_COUNT) >= activeConnectionMax
    case _ => false
  }
}
```

---

## Summary

The Celeborn Worker is a sophisticated data service that provides:

1. **4-Server Architecture**: Specialized servers for control, push, replicate, and fetch operations
2. **Multi-Tier Storage**: Memory → Local → HDFS → Cloud with automatic tiering
3. **Async Flushing**: Efficient data persistence with configurable thread pools
4. **Device Health Monitoring**: Proactive disk failure detection and handling
5. **Partition Management**: Complete lifecycle from reservation to commit to cleanup
6. **Traffic Control**: Memory-based backpressure and congestion control
7. **Replication**: Primary-replica pattern for fault tolerance

The design emphasizes:
- **Performance**: Async operations, zero-copy transfer, efficient buffer management
- **Reliability**: Health monitoring, graceful degradation, partition splitting
- **Scalability**: Multi-tier storage, configurable thread pools, backpressure
