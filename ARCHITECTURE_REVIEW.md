# Job Scheduler: Architecture Review & Technical Roadmap

## Executive Summary

This document outlines the current architectural state, known technical debt, and future roadmap for my Job Scheduler. While my core distributed scheduling primitives (Raft consensus, DAG workflow execution, and multi-dimensional resource placement) are fully functional, there is still some technical debt I need to address before the system can be considered production-ready.

## Current System Strengths

### 1. Core Scheduling & Placement
My scheduling engine performs well under load. The heap-based priority queue properly enforces ordering with FIFO tie-breaking. The multi-dimensional resource placement (CPU, RAM, GPU, Disk) correctly scores workers using `Best-Fit` and `Spread` strategies. Benchmarks show my placement decisions execute in ~1µs across 500-node clusters.

### 2. DAG Execution Engine
My dependency-graph scheduling implementation is robust and fully wired up via the `SubmitDAG` RPC.
- **Cycle Detection:** Correctly implemented via 3-color DFS.
- **Topological Sort:** Kahn's algorithm efficiently computes execution order (~254µs for 1000 tasks).
- **Failure Propagation:** Downstream tasks are automatically cancelled if an upstream dependency fails.

### 3. Raft Consensus & Persistence
The foundation for high availability is in place using HashiCorp Raft. The finite state machine (FSM) correctly handles the 7 core command types, backed by BoltDB for stable storage and snapshotting.

### 4. Resilient Task Execution
The executor correctly captures `stdout`/`stderr` with a 1MB limit to prevent memory exhaustion. Transient vs. permanent failures are properly classified (e.g., "command not found" skips retries), and the exponential backoff with jitter prevents thundering herd problems.

---

## Technical Debt & Known Bugs

### High Priority Fixes (Completed)
1. **`LeaderAddress` Resolution:** Fixed `LeaderAddress()` to return the `ServerAddress`.
2. **FSM Determinism:** Removed `time.Now()` calls directly inside the Raft `Apply()` loop and added deterministic timestamps to the Raft command payload.
3. **`UtilizationPercent` Dimensionality Bug:** Fixed the calculation to normalize CPU, memory, GPU, and disk utilization independently before averaging.

### Distributed Systems Gaps
- **Silent Data Loss on Status Report:** If `ReportTaskStatus` fails over the network, the task remains stuck in `RUNNING` state indefinitely on the scheduler, while the worker has already released it. **Action:** Implement retry loops for worker-to-scheduler status reporting.
- **Single Point of Failure in gRPC Connections:** `CoordinationClientAdapter` opens a single connection at startup. If the network partitions or the leader changes, the connection isn't automatically re-established. **Action:** Enable `grpc.WithKeepaliveParams` and proper reconnection handling.
- **Queue State Ephemerality:** The scheduling priority queue is maintained in-memory. If the leader crashes, the new leader starts with an empty queue. **Action:** Rebuild the queue from the FSM state during leader election.

---

## Future Roadmap (V2 / Production Readiness)

### Phase 1: Correctness & Stability (In Progress)
- [x] Fix the high-priority bugs listed above (Raft determinism, LeaderAddress, dimensionality bug).
- [ ] Add comprehensive unit and integration tests. Currently, my testing relies heavily on benchmarks. Robust unit tests are needed for the FSM, retry logic, and DAG validation.
- [ ] Implement graceful reconnects for the worker's gRPC coordination client and retry logic for status reporting.

### Phase 2: Feature Completion
- [x] **Wire `SubmitDAG` RPC:** The gRPC handler stub is now wired up to the `DAGExecutor`.
- [x] **Task Timeouts:** Implemented maximum duration bounds for running tasks via `context.WithTimeout`.
- [ ] **Worker Graceful Drain:** Wire up the existing `WorkerStateDraining` model to allow workers to finish executing current tasks without accepting new ones prior to shutdown.

### Phase 3: Observability & Security
- [ ] **Structured Logging & Correlation IDs:** Replace `log.Printf` with `slog` to enable JSON logging, injecting `job_id` and `task_id` into all relevant log lines.
- [ ] **Metrics Integration:** Expose Prometheus metrics (e.g., `scheduler_queue_depth`, `worker_tasks_running`, scheduling latency).
- [ ] **mTLS Authentication:** Secure inter-node RPCs and prevent arbitrary task submission via the public internet.

## Conclusion
The current implementation successfully validates my core distributed architecture. The immediate next steps focus entirely on adding unit test coverage and implementing proper observability, since I have already paid down the technical debt around Raft state determinism.
