# Celeborn Architecture Deep Dive Guide

This directory contains comprehensive technical deep dive guides for all major Celeborn components. These guides provide detailed implementation insights without requiring extensive code reading.

## Available Guides

### Core Components

1. **[Master Component](01-master-deep-dive.md)**
   - Initialization and lifecycle management
   - Slot allocation algorithms (Round Robin & Load Aware)
   - High availability implementation with Apache Ratis
   - Worker and shuffle metadata management
   - Internal state management and message handling

2. **[Worker Component](02-worker-deep-dive.md)**
   - 4-server architecture (Controller, Push, Replicate, Fetch)
   - Multi-tier storage system (Memory → Local → HDFS → Cloud)
   - Data push and fetch flows
   - Flusher system and device monitoring
   - Partition management and traffic control

## Guide Structure

Each guide follows a consistent structure:

1. **Architecture Overview**: High-level component design and responsibilities
2. **Implementation Details**: Key algorithms, data structures, and workflows
3. **Code References**: Specific file locations and line numbers for deep dives
4. **Flow Diagrams**: Visual representation of data and control flows
5. **Configuration**: Related configuration options and tuning parameters

## How to Use These Guides

### For New Contributors

1. **Start with the Overview**: Read the architecture section to understand high-level design
2. **Follow the Flows**: Study the data flow diagrams to see how components interact
3. **Deep Dive**: Use code references to explore specific implementations
4. **Experiment**: Try modifying configurations based on the tuning sections

### For Troubleshooting

1. **Identify the Component**: Determine which component is involved in your issue
2. **Check the Flow**: Follow the relevant data/control flow in the guide
3. **Review State Management**: Understand internal state and synchronization
4. **Apply Configuration**: Adjust settings based on the configuration sections

### For Performance Tuning

Each guide includes:
- **Thread Pool Configurations**: How to adjust concurrency
- **Memory Management**: Buffer sizes and memory pressure handling
- **Network Settings**: Timeout values and connection pooling
- **Storage Optimization**: Flusher threads and device selection

## Complementary Documentation

These deep dive guides complement the existing Celeborn documentation:

- **User Guides** (`docs/`): How to deploy and configure Celeborn
- **API Documentation**: RPC message definitions and interfaces
- **Contributor Guide** (`CONTRIBUTING_GUIDE.md`): Development workflow and standards

## Updates and Maintenance

These guides are maintained alongside the codebase. When making significant architectural changes:

1. Update the relevant deep dive guide
2. Add code references for new components
3. Update flow diagrams if control/data flows change
4. Document new configuration options

## Quick Reference

### Component Interaction Flow

```
Application
    │
    ├─> LifecycleManager ──> Master (Slot Allocation, HA)
    │                           │
    │                           ├─> Worker Management
    │                           └─> Metadata Management
    │
    └─> ShuffleClient ──┬─> Worker (Push Server) ──> Storage + Flusher
                        │           │
                        │           ├─> Replication to Peer Workers
                        │           └─> Device Monitoring
                        │
                        └─> Worker (Fetch Server) ──> Stream Management
```

### Key Files by Component

**Master**:
- `master/src/main/scala/org/apache/celeborn/service/deploy/master/Master.scala`
- `master/src/main/java/org/apache/celeborn/service/deploy/master/SlotsAllocator.java`
- `master/src/main/scala/org/apache/celeborn/service/deploy/master/clustermeta/ha/HARaftServer.scala`

**Worker**:
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/Worker.scala`
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/StorageManager.scala`
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/PushDataHandler.scala`
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/FetchHandler.scala`

## Feedback and Contributions

If you find areas that need clarification or additional detail:

1. File an issue on GitHub
2. Submit a PR with improvements
3. Discuss on the community Slack channel

---

**Last Updated**: November 2025
**Celeborn Version**: Main branch
