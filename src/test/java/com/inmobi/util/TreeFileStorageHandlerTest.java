package com.inmobi.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TreeFileStorageHandlerTest {
    
    private static final String filePath  = "/tmp/test.bin";
    private static final String filePath1 = "/tmp/test1.bin";
    
    @Test
    public void testDeleteAll() {
    
        StorageHandler<String> storageHandler = new TreeFileStorageHandler<String>(filePath1);
        storageHandler.put("testDeleteAll", "VALUE".getBytes());
        
        Assert.assertEquals("VALUE", new String(storageHandler.get("testDeleteAll")));
        storageHandler.deleteAll();
        Assert.assertNull(storageHandler.get("testDeleteAll"));
    }
    
    @Test
    public void testDuplicateValues() {
    
        StorageHandler<String> storageHandler = new TreeFileStorageHandler<String>(filePath);
        storageHandler.put("Test", "VALUE UP".getBytes());
        
        Assert.assertEquals("VALUE UP", new String(storageHandler.get("Test")));
    }
    
    @Test
    public void testGetPut() {
    
        StorageHandler<String> storageHandler = new TreeFileStorageHandler<String>(filePath);
        storageHandler.put("Test", "VALUE".getBytes());
        
        Assert.assertEquals("VALUE", new String(storageHandler.get("Test")));
    }
    
    @Test
    public void testIsFull() {
    
        StorageHandler<String> storageHandler = new TreeFileStorageHandler<String>(filePath);
        storageHandler.put("TEST", "value".getBytes());
        Assert.assertFalse(storageHandler.isFull());
    }
    
}
