# MemTable Implementation

## Overview

A MemTable is an in-memory data structure used in databases and storage systems to store key-value pairs temporarily before flushing them to disk. Different data structures can be used to implement a MemTable, each with its own advantages and disadvantages.

## Data Structures for MemTable

### 1. Skip List
- **Description**: A probabilistic data structure that allows fast search, insertion, and deletion operations.
- **Used by**: LevelDB, RocksDB
- **Pros**:
    - Logarithmic time complexity for operations.
    - Easy to implement and maintain.
    - Supports range queries efficiently.
- **Cons**:
    - Higher memory usage compared to balanced trees.
    - Performance depends on randomization.
    - Worst-case O(n) if unlucky (though rare).

### 2. Hash Table
- **Description**: A data structure that maps keys to values using a hash function.
- **Used by**: Some key-value stores (e.g. Redis) — but not usually in LSM trees
- **Pros**:
    - Constant time complexity for search, insertion, and deletion.
    - Simple and fast for exact key lookups.
- **Cons**:
    - Does not support range queries.
    - Hash collisions can degrade performance.
    - Requires resizing when the load factor increases.
    - No sorted order.

### 3. Balanced Binary Search Tree (e.g., AVL Tree, Red-Black Tree)
- **Description**: A self-balancing binary search tree that maintains sorted order of keys.
- **Used by**: WiredTiger (MongoDB), LMDB (as MemTable-like structure)
- **Pros**:
    - Logarithmic time complexity for operations.
    - Supports range queries and ordered traversal.
- **Cons**:
    - More complex to implement compared to Skip List.
    - Higher overhead for maintaining balance.

### 4. B-Tree
- **Description**: A generalization of binary search trees optimized for systems that read and write large blocks of data.
- **Pros**:
    - Efficient for disk-based storage systems.
    - Supports range queries and ordered traversal.
- **Cons**:
    - Higher complexity compared to Skip List and Hash Table.
    - Not ideal for purely in-memory use.

### 5. Trie / Radix Tree (for key prefixes)
- **Description**: A Trie (prefix tree) is a tree-like data structure used to store strings or sequences. It is particularly efficient for prefix-based searches.
- **Pros**:
  - Efficient for prefix-based lookups.
  - Supports ordered traversal of keys.
  - Can store large datasets with shared prefixes compactly.
- **Cons**:
  - Higher memory usage due to node overhead.
  - Not ideal for non-string keys.

### 6. Linked List
- **Description**: A sequential data structure where each element points to the next.
- **Pros**:
    - Simple to implement.
    - Efficient for append operations.
- **Cons**:
    - Linear time complexity for search and deletion.
    - Not suitable for large datasets.

### 7. Crash Safe Parallel Patricia Trie
- **Description**: A Crash Safe Parallel Patricia Trie is a specialized version of the Patricia Trie designed for high performance and crash recovery in concurrent environments. It is commonly used in databases and storage systems for efficient key-value storage.
- **Features**:
  - Crash Safety: Ensures data integrity during system crashes by using techniques like write-ahead logging or atomic updates.
  - Parallelism: Supports concurrent operations (insert, delete, search) by leveraging fine-grained locking or lock-free algorithms.
  - Compact Representation: Stores keys efficiently by compressing common prefixes.
- **Pros**:
  - Efficient for prefix-based searches.
  - Compact memory usage due to prefix compression.
  - Supports concurrent operations for high throughput.
  - Crash recovery ensures data integrity.
- **Cons**:
  - Complex implementation compared to simpler data structures.
  - Requires careful handling of concurrency and crash recovery mechanisms.
  - Higher overhead for maintaining crash safety.

## Choosing the Right Data Structure

The choice of data structure depends on the specific requirements of the system:
- **Skip List**: Best for systems requiring range queries and fast operations.
- **Hash Table**: Ideal for systems focused on exact key lookups.
- **Balanced Trees**: Suitable for systems requiring ordered traversal and range queries.
- **B-Tree**: Preferred for disk-based systems.
- **Trie**: suitable for applications like autocomplete or prefix matching.
- **Linked List**: Useful for simple append-only workloads.

## Example Implementation

This project uses a **Skip List** for the MemTable due to its balance between simplicity and performance.

## Requirements

- Java 8 or higher

## Build and Run

1. Clone the repository.
2. Compile the code using `javac`.
3. Run the `MemTable` class.

## References:
1. https://news.ycombinator.com/item?id=44432322

