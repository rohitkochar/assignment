package com.inmobi.util;

import java.io.Serializable;
import java.util.Iterator;

public interface StorageHandler<K extends Serializable & Comparable<K>> {
    
    void deleteAll();
    
    byte[] get(K key);
    
    Iterator<K> getIterator();
    
    boolean isFull();
    
    void put(K key, byte[] value);
}
