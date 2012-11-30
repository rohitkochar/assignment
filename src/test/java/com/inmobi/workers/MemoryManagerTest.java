package com.inmobi.workers;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.inmobi.util.MemoryStorageHandler;
import com.inmobi.util.StorageHandler;
import com.inmobi.util.TreeFileStorageHandler;

public class MemoryManagerTest {
    
    private static String  filePath = "/tmp/test.bin";
    ReentrantReadWriteLock lock     = new ReentrantReadWriteLock();
    Condition              notfull  = lock.writeLock().newCondition();
    
    @Test
    public void testRunWithHighMemoryLongDrainageTime() {
    
        long maxMemory = 5000;
        long drainageTime = 100;
        StorageHandler<String> memoryStorage = new MemoryStorageHandler<String>(maxMemory, lock, notfull);
        StorageHandler<String> fileStorage = new TreeFileStorageHandler<String>(filePath);
        MemoryManager<String> manager = new MemoryManager<String>(memoryStorage, fileStorage, lock, notfull, maxMemory,
                        drainageTime);
        
        memoryStorage.put("TESTING2", "value".getBytes());// memory shud not be
                                                          // full
        
        manager.run();// this should not shift content from memory to file
        Assert.assertNull(fileStorage.get("TESTING2"));
    }
    
    @Test
    public void testRunWithLowDrainageTime() throws InterruptedException {
    
        long maxMemory = 5000;
        long drainageTime = 1;
        StorageHandler<String> memoryStorage = new MemoryStorageHandler<String>(maxMemory, lock, notfull);
        StorageHandler<String> fileStorage = new TreeFileStorageHandler<String>(filePath);
        MemoryManager<String> manager = new MemoryManager<String>(memoryStorage, fileStorage, lock, notfull, maxMemory,
                        drainageTime);
        
        memoryStorage.put("TESTING1", "value".getBytes());// memory shud not be
                                                          // full
        Thread.sleep(2000); // wait more than drainage time
        manager.run();// this should shift content from memory to file
        Assert.assertEquals("value", new String(fileStorage.get("TESTING1")));
        
    }
    
    @Test
    public void testRunWithLowMemory() {
    
        long maxMemory = 5;
        long drainageTime = 10;
        StorageHandler<String> memoryStorage = new MemoryStorageHandler<String>(maxMemory, lock, notfull);
        StorageHandler<String> fileStorage = new TreeFileStorageHandler<String>(filePath);
        MemoryManager<String> manager = new MemoryManager<String>(memoryStorage, fileStorage, lock, notfull, maxMemory,
                        drainageTime);
        
        memoryStorage.put("TESTING", "value".getBytes());// memory shud be full
                                                         // now
        manager.run();// this should shift content from memory to file
        Assert.assertEquals("value", new String(fileStorage.get("TESTING")));
        
    }
}
