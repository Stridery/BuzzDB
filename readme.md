# BuzzDB

**BuzzDB** is an educational database system demo written in C++, designed to teach core concepts of database internals. The system is divided into four self-contained modules, each representing a key component in modern database systems: external sorting, write-ahead logging and crash recovery, two-phase locking with deadlock detection, and a cost-based query optimizer.

This project is ideal for students, instructors, or developers interested in hands-on implementation of database fundamentals.

---

## 🔧 Features

### 1️⃣ External Sort (Multi-Way Merge)

This module implements an **external sort** algorithm using a **multi-way merge** strategy, suitable for sorting datasets that exceed available memory.

#### ✅ Features

* Supports up to **9-way merge** from intermediate sorted runs
* Each chunk is **locally sorted in memory** using `std::sort`
* A **min-heap** (`std::priority_queue`) is used to track the current minimum across runs
* Merging is performed incrementally with partial chunks of each run held in memory
* Temporary files are used to store intermediate results and final output is written in blocks

#### 🧠 Key Implementation Details

* Memory-efficient design:

  ```cpp
  chunk_size = mem_size / ((ways + 2) * sizeof(uint64_t));
  ```

  Memory is split across `k` input buffers, 1 output buffer, and some overhead
* Input file is split into sorted chunks:

  ```cpp
  std::sort(chunk.get(), chunk.get() + read_size);
  chunk_file->write_block(...);
  ```
* Merge uses a min-heap to select the smallest current value:

  ```cpp
  priority_queue<pair<uint64_t, int>, ..., greater<>> pq;
  pq.push({chunk_pointer[i][0], i});
  ```
* Output is written in blocks to a temporary file, then flushed to the final output


### 2️⃣ Write-Ahead Logging and Recovery

This module implements the **Write-Ahead Logging (WAL)** protocol and supports full **crash recovery**, following the **ARIES-style** recovery workflow: **Analysis → Redo → Undo**.

#### ✅ Features

* Logs all transaction activity using fixed-size `LogRecord` structures
* Supports the following log types:

  * `BEGIN_RECORD`, `UPDATE_RECORD`, `COMMIT_RECORD`, `ABORT_RECORD`, `CHECKPOINT_RECORD`
* Tracks active transactions via an **Active Transaction Table (ATT)** and maintains a mapping of transaction start offsets
* Ensures durability and atomicity through **flush-on-checkpoint** and **before/after image logging**
* Performs **recovery** in three phases:

  1. **Analysis**: Reconstructs the set of active (uncommitted) transactions
  2. **Redo**: Reapplies updates from committed transactions using `after_img`
  3. **Undo**: Rolls back all updates from uncommitted transactions using `before_img`

#### 🧠 Key Implementation Highlights

```cpp
// Writing an update log record (with before/after images)
LogRecord record = {LogRecordType::UPDATE_RECORD, txn_id, page_id, length, offset};
std::memcpy(record.before_img, before_img, length);
std::memcpy(record.after_img, after_img, length);
write_log_record(record);
```

```cpp
// Redo: Apply after image from committed transactions
if (record.type == UPDATE_RECORD) {
  BufferFrame& frame = buffer_manager.fix_page(record.page_id, true);
  std::memcpy(&frame.get_data()[record.offset], record.after_img, record.length);
  buffer_manager.unfix_page(frame, true);
}
```

```cpp
// Undo: Roll back uncommitted updates using before image
if (record.txn_id == txn_id && record.type == UPDATE_RECORD) {
  BufferFrame& frame = buffer_manager.fix_page(record.page_id, true);
  std::memcpy(&frame.get_data()[record.offset], record.before_img, record.length);
  buffer_manager.unfix_page(frame, true);
}
```

#### 💡 Recovery Logic

* **Checkpoints** are logged periodically and all dirty pages are flushed:

  ```cpp
  buffer_manager.flush_all_pages();
  log_record = CHECKPOINT_RECORD;
  write_log_record(record);
  ```
* On system restart, recovery begins from the last checkpoint and performs:

  * **Forward scan** to redo committed updates
  * **Reverse scan** to undo incomplete transactions




### 3️⃣ Two-Phase Locking with Deadlock Detection

This module implements **Two-Phase Locking (2PL)** with full support for **shared (S)** and **exclusive (X)** locks, lock upgrades, transaction lifecycle management, and dynamic **deadlock detection** using a **wait-for graph**.

#### ✅ Features

* Implements strict **Two-Phase Locking (2PL)** to ensure serializability
* Supports:

  * **Shared (S)** locks
  * **Exclusive (X)** locks
  * **Lock upgrades** (S → X)
* Each transaction tracks its locked pages
* Uses a **lock table** for each page with:

  * A **granted queue**
  * A **request queue**
  * Mutex + condition variables for concurrency
* **Deadlock detection** via DFS on a dynamic **wait-for graph**
* Deadlock resolution by **transaction abort and lock release**

#### 🔐 Locking Protocol Logic

```cpp
// Shared vs Exclusive lock acquisition
LockMode mode = exclusive ? LockMode::EXCLUSIVE : LockMode::SHARED;
auto& lock = lock_table[page_id];
std::unique_lock<std::mutex> lk(lock.mutex);
```

```cpp
// Wait-for graph construction for deadlock detection
lock_manager.waits_for[txn_id].insert(conflicting_txn_id);
if (lock_manager.detect_deadlock(txn_id, visited)) {
    transaction_abort(txn_id);
    throw transaction_abort_error();
}
```

```cpp
// Lock upgrade logic (S → X)
if (lock.shared_holders.size() == 1 &&
    lock.shared_holders.count(txn_id) &&
    lock.exclusive_holder == 0 &&
    lock.upgrading_txn_id == txn_id) {
    // grant upgrade
}
```

#### 🔁 Transaction Lifecycle

* **Acquire Lock**: `fix_page(txn_id, page_id, exclusive)`
* **Release Lock**: `unfix_page(txn_id, frame, is_dirty)`
* **Abort Transaction**:

  * Roll back locks
  * Discard pages
  * Notify waiting transactions
* **Complete Transaction**:

  * Flush dirty pages
  * Release locks
  * Notify queue



### 4️⃣ Cost-Based Query Optimizer

This module implements a **cost-based query optimizer** that chooses an optimal join order based on estimated I/O and CPU costs. The optimizer uses **Selinger-style dynamic programming**, **histogram-based selectivity estimation**, and **join cardinality estimation** to generate an efficient **left-deep join plan**.

#### ✅ Features

* Computes join cost using a **nested loop join cost model**:

  ```
  cost = scan_cost(t1) + card(t1) × scan_cost(t2) + card(t1) × card(t2)
  ```
* Estimates join output size (cardinality) using primary key information or a fallback heuristic
* Constructs **IntHistogram** structures to estimate **predicate selectivity**
* Applies **filter selectivity** to compute expected cardinality
* Explores all possible join orderings using dynamic programming over subsets

#### 🧠 Key Implementation Highlights

```cpp
// Cost model for nested loop join
double JoinOptimizer::estimate_join_cost(...) {
    return cost1 + card1 * cost2 + card1 * card2;
}
```

```cpp
// Cardinality estimation
int JoinOptimizer::estimate_join_cardinality(...) {
    if (t1pkey && !t2pkey) return card2;
    if (!t1pkey && t2pkey) return card1;
    if (t1pkey && t2pkey) return min(card1, card2);
    return 0.3 * card1 * card2;
}
```

```cpp
// Selectivity estimation using histograms
double IntHistogram::estimate_selectivity(PredicateType op, int64_t val);
```

```cpp
// Dynamic programming for join ordering
for (size_t i = 1; i <= _joins.size(); ++i) {
    auto subsets = enumerate_subsets(_joins, i);
    for (const auto& subset : subsets) {
        compute_cost_and_card_of_subplan(...);
        pc.add_plan(...);
    }
}
```

#### 📦 Stats Module

* Builds **table-level histograms** in two passes using `SeqScan`
* Captures `min`, `max`, and tuple distribution for each column
* Used to estimate filter selectivities:

  ```cpp
  selectivity = histogram.estimate_selectivity(PredicateType::GT, 5);
  ```


---

## ⚙️ Tech Stack

- **Language**: C++
- **Build System**: CMake
- **Platform**: Linux / macOS / WSL2 / Windows (with MSYS2)

---
