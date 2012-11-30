package com.inmobi.workers;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.inmobi.util.StorageHandler;

public class MemoryManager<K extends Serializable & Comparable<K>> implements Runnable {
    
    private final ReentrantReadWriteLock lock;
    private final Condition              notfull;
    private long                         lastrun = 0;
    private final StorageHandler<K>      memoryStorage;
    private final StorageHandler<K>      fileStorage;
    private final long                   maxMemory;
    private final long                   drainageTime;
    private final Logger                 logger  = Logger.getLogger(getClass());
    
    public MemoryManager(StorageHandler<K> memoryStorage, StorageHandler<K> fileStorage, ReentrantReadWriteLock lock,
                    Condition notfull, long maxMemory, long drainageTime) {
    
        this.lock = lock;
        this.notfull = notfull;
        this.memoryStorage = memoryStorage;
        this.fileStorage = fileStorage;
        this.maxMemory = maxMemory;
        this.drainageTime = drainageTime;
        lastrun = System.currentTimeMillis();
        
    }
    
    @Override
    public void run() {
    
        long currentTime = System.currentTimeMillis();
        logger.debug("Starting run of worker at " + currentTime);
        if (((currentTime - lastrun) >= drainageTime) || memoryStorage.isFull()) {
            if ((currentTime - lastrun) >= drainageTime) {
                logger.debug("Running worker as its been not run for long");
            } else {
                logger.debug("Running worker as memory is full");
            }
            // flushing memory to file
            try {
                logger.debug("Acquiring write lock in the worker");
                lock.writeLock().lock();// write lock would prevent any get/put
                                        // operations to memory store
                Iterator<K> iterator = memoryStorage.getIterator();
                while (iterator.hasNext()) {
                    K key = iterator.next();
                    fileStorage.put(key, memoryStorage.get(key));
                }
                memoryStorage.deleteAll();
                
                logger.debug("Signalling other thread blocked on memory availability");
                notfull.signal();
                logger.debug("Updating last run time of worker");
                lastrun = currentTime;
            } finally {
                logger.debug("Write lock released in the worker");
                lock.writeLock().unlock();
            }
        }
        
    }
}
