package com.memtable;

/**
 * MemTable is an in-memory data structure that provides fast access to key-value pairs.
 * It uses a SkipList as the underlying data structure for efficient operations.
 *
 * @param <K> The type of keys, which must be comparable.
 * @param <V> The type of values associated with the keys.
 */
public class MemTable<K extends Comparable<K>, V> {

    private SkipList<K, V> skipList = new SkipList<>();

    public void put(K key, V value) {
        skipList.put(key, value);
    }

    public V get(K key) {
        return skipList.get(key);
    }

    public boolean delete(K key) {
        return skipList.delete(key);
    }
}
