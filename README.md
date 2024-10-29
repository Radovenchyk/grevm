# Parallel EVM Roadmap

# Why Most Parallel EVM Implementations Underperform

Although the Block-STM framework for parallel execution has seen initial applications in Aptos and Solana, its adoption within EVM-compatible blockchains has been limited. During Gravity’s early testing phases, we found that conventional Parallel Execution frameworks often fall short, primarily due to the following issues:

1. Limited and imprecise state hints, which fail to accurately infer transaction dependencies.
    - Reference: [BNB Chain Parallel EVM Implementation](https://www.bnbchain.org/en/blog/road-to-high-performance-parallel-evm-for-bnb-chain)
2. Extensive I/O during transaction execution, significantly increasing latency compared to in-memory execution.
    - Each account or storage slot operation can introduce ms-level delays; even with *State Storage* and *State Commitment* separation, latency remains at several hundred microseconds.
    - With continuous data growth, the cache hit rate for accounts and storage slots declines markedly.
    - State bloat exacerbates resource competition among CPU, memory, and disk bandwidth, reducing the resources available for transaction execution.

For example, a typical ERC20 transfer on Ethereum involves a gas cost of around 50,000, modifying two accounts and two storage slots. Pure in-memory execution takes approximately 100 microseconds, whereas cache misses during I/O often require several hundred microseconds to milliseconds. Although Reth’s storage model reduces I/O latency compared to Geth, transaction time in I/O-intensive operations still poses significant performance bottlenecks.

1. Optimistic execution models are unsuitable for EVM, an interpreted virtual machine, due to frequent redundant executions in high-activity or hot spots, especially with large blocks, leading to considerable resource waste.

# Ideal Parallel EVM Model

An ideal Parallel EVM model should incorporate the following characteristics, as shown below:
![image](https://github.com/user-attachments/assets/891358ec-12be-4ace-b4fd-79eb49e64e87)


**Characteristics of the Ideal Parallel EVM Model:**

1. Efficient resource utilization of CPU, RAM, and disk bandwidth.
2. Minimal context-switching overhead and reduced finality latency.
3. Elimination of redundant execution.

# Gravity’s Parallel EVM Framework

To address the challenges identified, the Gravity Team has enhanced the Parallel EVM framework based on the Block-STM foundation. We divide the parallel execution process into three main phases:

### Phase 1: Hint Generation

Prior to executing transactions in Parallel EVM, GravityChain collects the most accurate and comprehensive transaction hints possible to form transaction dependencies. Key methods for generating hints include:

- **Mempool Transaction Inspection and Forwarding**: Using simulate run against the latest view to retrieve hints.
- **Static Analysis**: Identifies potential conflicts in complex transactions before execution. While static analysis cannot prevent all conflicts, it effectively identifies dependencies to enable more efficient parallel execution.
- **Transaction Speculation**: By analyzing contract templates and transaction payloads, read-write sets can often be predicted. This is particularly effective for contracts like ERC20/ERC721. For hotspot contracts (e.g., DEXs and lending protocols), off-chain hint indices may be established to quickly construct dependencies, potentially aided by AI tools.

### Phase 2: Preloading State

![image](https://github.com/user-attachments/assets/17490194-b9cf-4a7a-8762-0a9cd1c22653)
*Figure 2: Distribution of Ethereum State (source: [https://www.paradigm.xyz/2024/03/how-to-raise-the-gas-limit-1](https://www.paradigm.xyz/2024/03/how-to-raise-the-gas-limit-1))*

With around 250 GB of state data, it is impossible to load the entire state into memory on standard hardware. Beyond traditional LRU caching, maximizing disk I/O utilization and preloading relevant state storage for Parallel EVM is crucial.

Therefore, at the outset of the Pipeline and Parallel EVM processes, Gravity prioritizes preloading target states into memory (mirroring Gravity's Parallel State Calculation design).

### Phase 3: Multithreaded Asynchronous Concurrency

In Gravity’s Parallel EVM framework, a Scheduler dispatches transactions to the thread pool and manages dependencies, validation, and cache management.

Using Block-STM principles, GravityChain partitions each RawBlock into multiple segments (partitions) based on transaction dependencies, with each partition acting as a sub-scheduler. When all read-write sets within a partition remain non-overlapping, transactions execute without conflict. Even if overlaps occur, they pose no conflict if there is no intersection with other partitions. Within each partition, parallel execution can also take place.

This setup minimizes load on the Main Scheduler and Cache Manager, thus avoiding potential performance bottlenecks.

### Phase 4: Convergence

In cases of unexpected partition overlaps, the Main Scheduler employs the following mechanisms to adjust concurrency within the thread pool:

- **Block Partitioning**: Sets a partition to a “block” status (or potentially triggers a rollback), awaiting finalization of a specific transaction before resuming execution.
- **Re-Partitioning**: Reassigns conflicting partitions as needed, bypassing transactions that have already achieved finality to avoid redundant processing.

---

# Parallel EVM 1.0

![image](https://github.com/user-attachments/assets/e8cb43c4-7e39-4ae8-8fca-6d1d071aa3a6)

In Parallel EVM 1.0, we implement the above four phases across multiple rounds, as shown:

1. Before transaction execution, simulate execution, static analysis, and speculative transaction prediction are used to generate hints.
2. Based on dependencies (Read after Write), transactions are organized into a directed acyclic graph (DAG), with connectivity used to create multiple partitions (default thread count *2 + 1).
3. In the first round, Parallel Execution generates more accurate hints, enhancing re-partitioning precision for the next round while also preloading state into memory for faster subsequent rounds.
4. Each round records transactions in Conflict, Pending, or Finalized status. If convergence occurs within three rounds, processing returns to parallel execution; otherwise, it defaults to serial execution to maintain consistency.

# Parallel EVM 2.0

## Architectural Overview

![image](https://github.com/user-attachments/assets/cd423724-acef-41b2-9fcc-3b5a1295e800)

In version 2.0, Parallel EVM introduces a finer-grained, transaction-level concurrency control using a fully asynchronous model. Each partition is represented by a lightweight logical table. The Scheduler dynamically selects and assigns transactions from each partition based on its state, preventing redundant rounds and allowing convergence within a single round of parallel execution.

This architecture significantly optimizes execution, minimizes unnecessary reprocessing, and enhances overall throughput for high-performance blockchain applications on Gravity.
