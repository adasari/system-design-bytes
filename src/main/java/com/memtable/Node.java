package com.memtable;

public class Node<K extends Comparable<K>, V> {
    K key;
    V value;

    Node<K, V>[] forward;

    public Node(K key, V value, int level) {
        this.key = key;
        this.value = value;
        this.forward = new Node[level + 1];
    }
}
