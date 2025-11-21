# Consistent Hashing Implementation

This project implements a consistent hashing algorithm in Java. Consistent hashing is a technique used in distributed systems to distribute keys (e.g., data, requests) across a set of nodes (e.g., servers) in a way that minimizes key redistribution when nodes are added or removed.

## Features

- **Virtual Nodes**: Each physical node is represented by multiple virtual nodes to improve key distribution.
- **Key Assignment**: Keys are assigned to nodes based on their hash values.
- **Rebalancing**: Keys are redistributed when nodes are added or removed.
- **MD5 Hashing**: Uses MD5 for generating hash values.

## Classes

### `Node`
Represents a physical node in the system.

### `VNode`
Represents a virtual node mapped to a physical node.

### `ConsistentHashing`
The main class that implements the consistent hashing algorithm.

## How It Works

1. **Hash Ring**: Nodes and keys are placed on a circular hash ring based on their hash values.
2. **Key Assignment**: Each key is assigned to the closest node (successor) on the ring.
3. **Rebalancing**: When a node is added or removed, only the keys in the affected range are reassigned.

## Usage

### Adding Nodes
```java
ConsistentHashing ch = new ConsistentHashing(3); // 3 virtual nodes per physical node
Node nodeA = new Node("NodeA");
ch.addNode(nodeA);
```

### Adding Keys
```java
ch.addKey("user1");
ch.addKey("user2");
```

### Removing Nodes
```java
ch.removeNode(nodeA);
```

### Display the Ring
```java
ch.showRing();
```

### Displaying Key Assignments
```java
ch.showKeyAssignments();
```

## Requirements

* Java 8 or higher

## Build and Run

1. Clone the repository
2. Compile the code and using `javac`
3. Run the `ConsistentHashing` class.