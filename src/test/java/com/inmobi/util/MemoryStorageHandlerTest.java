package com.inmobi.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.inmobi.util.MemoryStorageHandler;
import com.inmobi.util.StorageHandler;

public class MemoryStorageHandlerTest {
    
    private final ReentrantReadWriteLock lock    = new ReentrantReadWriteLock();
    Condition                            notfull = lock.writeLock().newCondition();
    
    @Test
    public void testDeleteAll() {
    
        StorageHandler<String> storageHandler = new MemoryStorageHandler<String>(100, lock, notfull);
        storageHandler.put("Test", "VALUE".getBytes());
        
        Assert.assertEquals("VALUE", new String(storageHandler.get("Test")));
        storageHandler.deleteAll();
        Assert.assertNull(storageHandler.get("Test"));
    }
    
    @Test
    public void testDuplicateValues() {
    
        StorageHandler<String> storageHandler = new MemoryStorageHandler<String>(100, lock, notfull);
        storageHandler.put("Test", "VALUE UP".getBytes());
        
        Assert.assertEquals("VALUE UP", new String(storageHandler.get("Test")));
    }
    
    @Test
    public void testGetPut() {
    
        StorageHandler<String> storageHandler = new MemoryStorageHandler<String>(100, lock, notfull);
        storageHandler.put("Test", "VALUE".getBytes());
        
        Assert.assertEquals("VALUE", new String(storageHandler.get("Test")));
    }
    
    @Test
    public void testIsFull() {
    
        StorageHandler<String> storageHandler = new MemoryStorageHandler<String>(1, lock, notfull);
        storageHandler.put("TEST", "value".getBytes());
        Assert.assertTrue(storageHandler.isFull());
    }
    
    @Test
    public void testIterator() {
    
        StorageHandler<String> storageHandler = new MemoryStorageHandler<String>(100, lock, notfull);
        ArrayList<String> list = new ArrayList<String>();
        list.add("Test");
        storageHandler.put("Test", "VALUE".getBytes());
        list.add("Test1");
        storageHandler.put("Test1", "VALUE".getBytes());
        list.add("Test2");
        storageHandler.put("Test2", "VALUE".getBytes());
        
        Iterator<String> iterator = storageHandler.getIterator();
        while (iterator.hasNext()) {
            Assert.assertTrue(list.contains(iterator.next()));
        }
    }
}
