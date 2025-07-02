package com.memtable;

import java.util.Random;

/**
 * SkipList is a probabilistic data structure that allows fast search, insertion, and deletion operations.
 * It uses multiple levels of linked lists to achieve logarithmic time complexity for these operations.
 *
 * @param <K> The type of keys, which must be comparable.
 * @param <V> The type of values associated with the keys.
 */
public class SkipList<K extends Comparable<K>, V> {

    private static final int MAX_LEVEL = 8; // Maximum number of levels in the skip list
    private static final double P = 0.5; // Probability factor for level generation

    // Head node of the skip list
    private final Node<K, V> head = new Node<>(null, null, MAX_LEVEL);
    // Current maximum level in the skip list
    private int level = 0;

    private final Random random = new Random();

    /**
     * Generates a random level for a new node based on the probability factor.
     *
     * @return The level for the new node.
     */
    private int getRandomLevel() {
        int l = 0;

        while (random.nextDouble() < P && l < MAX_LEVEL) {
            l++;
        }

        return l;
    }

    /**
     * Retrieves the value associated with the given key.
     *
     * @param key The key to search for.
     * @return The value associated with the key, or null if the key is not found.
     */
    public V get(K key) {
        Node<K, V> current = head;

        for (int i = level; i >= 0; i--) {
            while (current.forward[i] != null && current.forward[i].key.compareTo(key) < 0) {
                current = current.forward[i];
            }
        }

        current = current.forward[0];
        if (current != null && current.key.equals(key)) {
            return current.value;
        }

        return null;
    }

    /**
     * Inserts or updates a key-value pair in the skip list.
     *
     * @param key   The key to insert or update.
     * @param value The value associated with the key.
     */
    public void put(K key, V value) {
        // update stores the levels that requires update.
        Node<K, V>[] update = new Node[MAX_LEVEL + 1];
        Node<K, V> current = head;

        // Traverse levels to find the position for the new key
        for (int i = level; i >=0; i--) {
            while (current.forward[i] != null && current.forward[i].key.compareTo(key) < 0 ) {
                current = current.forward[i];
            }

            update[i] = current;
        }

        current = current.forward[0];

        // Update the value if the key already exists
        if (current != null && current.key.equals(key)) {
            current.value = value;
        } else {
            int newLevel = getRandomLevel();
            if (newLevel > level) {
                for (int i = level + 1; i <= newLevel; i++) {
                    update[i] = head; // new level default is default head.
                }

                level = newLevel;
            }

            Node<K, V> newNode = new Node<>(key, value, newLevel);
            for (int i = 0; i <= newLevel; i++) {
                newNode.forward[i] = update[i].forward[i];
                update[i].forward[i] = newNode;
            }
        }
    }

    /**
     * Deletes a key-value pair from the skip list.
     *
     * @param key The key to delete.
     * @return True if the key was deleted, false if the key was not found.
     */
    public boolean delete(K key) {
        Node<K, V>[] update = new Node[MAX_LEVEL + 1];
        Node<K, V> current = head;

        // Traverse levels to find the key
        for (int i = level; i >=0; i--) {
            while (current.forward[i] != null && current.forward[i].key.compareTo(key) < 0 ) {
                current = current.forward[i];
            }

            update[i] = current;
        }

        current = current.forward[0];

        // Remove the node if the key exists
        if (current != null && current.key.equals(key)) {
            // element exist?
            for (int i = 0; i <= level; i++) {
                // update the current element references at every level.
                if (update[i].forward[i] != current) break;
                update[i].forward[i] = current.forward[i];
            }

            // Adjust the level if necessary
            while (level > 0 && head.forward[level] == null) {
                level--;
            }

            return true;
        }

        return false;
    }
}
