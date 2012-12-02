package com.inmobi.assignment;

import java.io.Serializable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.inmobi.exceptions.InitializationException;
import com.inmobi.util.MemoryStorageHandler;
import com.inmobi.util.StorageHandler;
import com.inmobi.util.TreeFileStorageHandler;
import com.inmobi.workers.MemoryManager;

public class StoreImpl<K extends Serializable & Comparable<K>> implements Store<K> {
    
    private long                         maxMemory;
    private long                         timeForDrainage;
    private long                         timeForMemoryHandler;
    private boolean                      isInitialized = false;
    private ScheduledExecutorService     executor;
    private MemoryManager<K>             manager;
    private StorageHandler<K>            memoryStorage;
    private StorageHandler<K>            fileStorage;
    
    // this lock and condition is used to synchronize between memory storage and
    // memory handler
    
    private final ReentrantReadWriteLock lock          = new ReentrantReadWriteLock();
    private final Condition              notfull       = lock.writeLock().newCondition();
    private Logger                       logger;
    private String                       persistentStore;
    
    private void configure(String configFilePath) throws org.apache.commons.configuration.ConfigurationException {
    
        PropertyConfigurator.configure("src/main/resources/log4j.properties");
        PropertiesConfiguration config = new PropertiesConfiguration();
        logger = Logger.getLogger(this.getClass());
        logger.debug("Loading config from " + configFilePath);
        config.load(configFilePath);
        // maxMemory in bytes
        maxMemory = config.getLong("maxMemory", 10000);
        // max time allowed between consecutive writes of memory to persistent
        // store, in seconds
        timeForDrainage = config.getLong("timeForDrainage", 30);
        timeForMemoryHandler = config.getLong("timeForMemoryHandler", 5);
        logger.debug("Max memory usage allowed is " + maxMemory + " bytes" + "  and time interval for drainage is "
                        + timeForDrainage + "seconds");
        persistentStore = config.getString("persistentStore", "/tmp/store.bin");
    }
    
    public void destroy() {
    
        logger.debug("Shutting down the executor service");
        if (executor != null) {
            executor.shutdown();
        }
    }
    
    public byte[] get(K key) throws InitializationException {
    
        byte[] value;
        if (!isInitialized) throw new InitializationException("Store is not initialized");
        if ((value = memoryStorage.get(key)) != null) {
            logger.debug("Value for key = " + key + " returned from inmemory");
            return value;
        } else {
            logger.debug("Value for key = " + key + " returned from persistent store");
            return fileStorage.get(key);
        }
    }
    
    public void init(String configFilePath) throws InitializationException {
    
        try {
            configure(configFilePath);
        } catch (ConfigurationException e) {
            throw new InitializationException("Exception while init");
        }
        memoryStorage = new MemoryStorageHandler<K>(maxMemory, lock, notfull);
        fileStorage = new TreeFileStorageHandler<K>(persistentStore);
        
        executor = new ScheduledThreadPoolExecutor(1);
        manager = new MemoryManager<K>(memoryStorage, fileStorage, lock, notfull, timeForDrainage);
        // scheduling memory management task
        executor.scheduleWithFixedDelay(manager, 0, timeForMemoryHandler, TimeUnit.SECONDS);
        isInitialized = true;
    }
    
    public void put(K key, byte[] value) throws InitializationException {
    
        if (!isInitialized) throw new InitializationException("Store is not initialized");
        logger.debug("Adding key = " + key + " to memory store");
        memoryStorage.put(key, value);
        logger.debug("Added key = " + key + " to memory store");
        
    }
    
}
