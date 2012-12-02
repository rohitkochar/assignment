package com.inmobi.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

/*
 * Sum of the size of all the values stored in memory is used as total size of memory used
 * It ignores other overheads associated with data storage like size of keys,object overhead for map etc
 */
public class MemoryStorageHandler<K extends Serializable & Comparable<K>> implements StorageHandler<K> {
    
    private final long                   maxMemory;
    // This member needs to be volatile as its value could be altered from
    // another thread
    volatile private long                totalSize = 0;
    private final ReentrantReadWriteLock lock;
    private final Condition              notfull;
    private final Logger                 logger    = Logger.getLogger(this.getClass());
    private Map<K, byte[]>               storage   = new ConcurrentHashMap<K, byte[]>();
    
    public MemoryStorageHandler(long maxMemory, ReentrantReadWriteLock lock, Condition notfull) {
    
        this.maxMemory = maxMemory;
        this.lock = lock;
        this.notfull = notfull;
    }
    
    public void deleteAll() {
    
        // Need not use concurrent map here as already a write lock is acquired
        // before the put operation.
        storage = new HashMap<K, byte[]>();
        totalSize = 0;
        logger.debug("Deleted all the keys from in memory store");
    }
    
    public byte[] get(K key) {
    
        try {
            logger.debug("Taking the read lock for fetching the key from in memory store for key =" + key);
            lock.readLock().lock();
            return storage.get(key);
        } finally {
            lock.readLock().unlock();
            logger.debug("Releasing the read lock after fetching from in memory store for key =" + key);
        }
    }
    
    public Iterator<K> getIterator() {
    
        return storage.keySet().iterator();
    }
    
    public boolean isFull() {
    
        if (totalSize >= maxMemory) return true;
        return false;
    }
    
    /*
     * Assumption:null is not a valid value as map is being used for storage of
     * key value
     */
    
    public void put(K key, byte[] value) {
    
        try {
            // write lock has been taken to assure that while this is write is
            // in
            // progress memoryhandler should not drain its content
            logger.debug("Acquiring write lock before adding key=" + key);
            lock.writeLock().lock();
            /*
             * wait in while loop as
             * sometime all threads are invoked accidentally by JVM
             */
            while (totalSize >= maxMemory) {
                try {
                    logger.debug("Memory is full....Waiting for the worker to run before adding key =" + key);
                    notfull.await();
                } catch (InterruptedException e) {
                    logger.error("Memorystorage thread interrupted", e);
                }
                logger.debug("Worker has run..proceeding with putting key= " + key);
            }
            
            logger.debug("Adding key =" + key + " to inmemory");
            storage.put(key, value);
            totalSize += value.length;
            logger.debug("Total size of memory used is =" + totalSize);
        } finally {
            lock.writeLock().unlock();
            logger.debug("Write lock released for key= " + key);
        }
        
    }
    
}
