package com.inmobi.assignment;

import java.io.Serializable;

import com.inmobi.exceptions.InitializationException;

/**
 * ASSUMPTION:Keys are comparable as well.this is needed for efficient storage
 * in a file
 **/
public interface Store<K extends Serializable & Comparable<K>> {
    
    void destroy();
    
    byte[] get(K key) throws InitializationException;
    
    void init(String configFilePath) throws InitializationException;
    
    void put(K key, byte[] value) throws InitializationException;
}