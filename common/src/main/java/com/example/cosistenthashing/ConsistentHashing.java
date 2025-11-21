package com.cosistenthashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashing {

    // Represents a physical node in the system
    static class Node {
        private String host;

        public Node(String host) {
            this.host = host;
        }

        public String getHost() {
            return this.host;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return Objects.equals(host, node.host);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(host);
        }

        @Override
        public String toString() {
            return "Node{" +
                    "host='" + host + '\'' +
                    '}';
        }
    }

    // Represents a virtual node (VNode) mapped to a physical node
    class VNode {
        private Node node;
        private int hash;
        private int id;

        public VNode(int hash, Node node, int id) {
            this.node = node;
            this.hash = hash;
            this.id = id;
        }

        public Node getNode() {
            return node;
        }

        public int getHash() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            VNode vNode = (VNode) o;
            return hash == vNode.hash && Objects.equals(node, vNode.node);
        }

        @Override
        public int hashCode() {
            return Objects.hash(node, hash);
        }

        @Override
        public String toString() {
            return "VNode{" +
                    "node=" + node +
                    ", hash=" + hash +
                    ", id=" + id +
                    '}';
        }
    }

    // Number of virtual nodes per physical node
    private final int virtualNodeCount;

    // Hash ring to store virtual nodes
    private final TreeMap<Integer, VNode> ring = new TreeMap<>();
    // Mapping of physical nodes to their virtual nodes
    private final Map<String, List<VNode>> nodeToVNodes = new HashMap<>();

    // Mapping of keys to virtual node hashes
    private final Map<String, Integer> keyToVnodeHash = new HashMap<>();

    // Mapping of virtual node hashes to their assigned keys
    private final Map<Integer, Set<String>> vnodeToKeys = new HashMap<>();

    public ConsistentHashing(int virtualNodeCount) {
        this.virtualNodeCount = virtualNodeCount;
    }

    // Add a physical node to the hash ring
    public List<VNode> addNode(Node node) {
        if (nodeToVNodes.containsKey(node.getHost())) {
            throw new IllegalArgumentException("Node already exists: " + node.getHost());
        }

        List<VNode> vnodes = new ArrayList<>();
        for (int i = 0; i < virtualNodeCount; i++) {
            String virtualNodeId = node.getHost() + "-" + i;
            int vnodeHash = hash(virtualNodeId);

            if (ring.containsKey(vnodeHash)) {
                throw new IllegalArgumentException("Hash collision at: " + vnodeHash);
            }

            VNode vnode = new VNode(vnodeHash, node, i);

            ring.put(vnodeHash, vnode);
            vnodeToKeys.put(vnodeHash, new HashSet<>());
            vnodes.add(vnode);
        }

        nodeToVNodes.put(node.getHost(), vnodes);
        rebalanceOnAdd(vnodes);
        return vnodes;
    }

    // Remove a physical node from the hash ring
    public void removeNode(Node node) {
        List<VNode> vnodes = nodeToVNodes.remove(node.getHost());
        if (vnodes == null) {
            throw new IllegalArgumentException("Node does not exist: " + node.getHost());
        }

        for (VNode vnode : vnodes) {
            ring.remove(vnode.getHash());
        }

        rebalanceOnRemove(vnodes);
        System.out.println("Node removed: " + node.getHost());
    }

    // Add a key to the hash ring
    public void addKey(String key) {
        int vnodeHash = findVnodeForHash(hash(key));
        keyToVnodeHash.put(key, vnodeHash);
        vnodeToKeys.get(vnodeHash).add(key);

        Node assignedNode = ring.get(vnodeHash).getNode();
        System.out.printf("Key '%s' assigned to %s%n", key, assignedNode.getHost());
    }

    // Find the vnode hash that owns the key hash (successor vnode on ring)
    private int findVnodeForHash(int keyHash) {
        SortedMap<Integer, VNode> tailMap = ring.tailMap(keyHash);
        if (!tailMap.isEmpty()) {
            return tailMap.firstKey();
        }
        return ring.firstKey();
    }

    // Rebalance keys when new virtual nodes are added
    private void rebalanceOnAdd(List<VNode> newVnodes) {
        for (VNode vNode : newVnodes) {
            int hash = vNode.getHash();
            int prev = ring.lowerKey(hash) != null ? ring.lowerKey(hash) : ring.lastKey();
            reassignKeys(prev, hash);
        }
    }

    // Rebalance keys when virtual nodes are removed
    private void rebalanceOnRemove(List<VNode> removedVnodes) {
        for (VNode vnode : removedVnodes) {
            int vnodeHash = vnode.getHash();
            Set<String> keysToMove = vnodeToKeys.remove(vnodeHash);
            if (keysToMove == null || keysToMove.isEmpty()) continue;

            int newHash = findVnodeForHash(vnodeHash);
            // add keys to new vnode.
            vnodeToKeys.get(newHash).addAll(keysToMove);

            for (String key : keysToMove) {
                keyToVnodeHash.put(key, newHash);
                String from = vnode.getNode().getHost();
                String to = ring.get(newHash).getNode().getHost();
                if (!from.equals(to)) {
                    System.out.printf("Key '%s' moved from %s → %s%n", key, from, to);
                }
            }
        }
    }

    // Reassign keys between two vnode ranges
    private void reassignKeys(int prevHash, int newHash) {
        Set<String> keysToCheck = vnodeToKeys.get(prevHash);
        if (keysToCheck == null || keysToCheck.isEmpty()) return;

        Iterator<String> iterator = keysToCheck.iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            int keyHash = hash(key);
            if (inRange(prevHash, newHash, keyHash)) {
                // This key should move to new vnode at endHash
                Node oldNode = ring.get(prevHash).getNode();
                Node newNode = ring.get(newHash).getNode();

                // Move key mapping
                iterator.remove();
                keyToVnodeHash.put(key, newHash);
                vnodeToKeys.get(newHash).add(key);

                if (!oldNode.getHost().equals(newNode.getHost())) {
                    System.out.printf("  Key '%s' moved from %s → %s%n", key, oldNode.getHost(), newNode.getHost());
                }
            }
        }
    }

    // Check if a value is in a specific range on the hash ring
    private boolean inRange(int start, int end, int value) {
        if (start < end) {
            return value > start && value <= end;
        } else {
            return value > start || value <= end;
        }
    }

    // Generate a hash for a given key using MD5
    private int hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes());
            return ((digest[0] & 0xFF) << 24) |
                    ((digest[1] & 0xFF) << 16) |
                    ((digest[2] & 0xFF) << 8) |
                    (digest[3] & 0xFF);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }

    // Display the current hash ring with virtual nodes
    public void showRing() {
        System.out.println("Hash Ring (Virtual Nodes):");
        for (Map.Entry<Integer, VNode> entry : ring.entrySet()) {
            System.out.println("  " + entry.getKey() + " → " + entry.getValue());
        }
    }

    // Display the keys assigned to each node
    public void showKeyAssignments() {
        Map<String, Set<String>> nodeToKeys = new HashMap<>();
        for (Map.Entry<Integer, Set<String>> entry : vnodeToKeys.entrySet()) {
            VNode node = ring.get(entry.getKey());
            nodeToKeys.computeIfAbsent(node.toString(), k -> new HashSet<>()).addAll(entry.getValue());
        }

        System.out.println("Key Assignments:");
        for (Map.Entry<String, Set<String>> entry : nodeToKeys.entrySet()) {
            System.out.println("  Node " + entry.getKey() + ": " + entry.getValue());
        }
    }


    public static void main(String[] args) {
        ConsistentHashing ch = new ConsistentHashing(3); // 3 virtual nodes per node

        Node nodeA = new Node("NodeA");
        Node nodeB = new Node("NodeB");

        ch.addNode(nodeA);
        ch.addNode(nodeB);

        for (String key : List.of("user1", "user2", "user3", "user4", "user5", "user6")) {
            ch.addKey(key);
        }

        ch.showKeyAssignments();
        System.out.println();

        Node nodeC = new Node("NodeC");
        ch.addNode(nodeC); // keys move only if in vnode ranges affected

        System.out.println();
        ch.showKeyAssignments();
        System.out.println();

        ch.removeNode(nodeB); // keys move only for removed node's vnodes

        System.out.println();
        ch.showKeyAssignments();
    }
}
