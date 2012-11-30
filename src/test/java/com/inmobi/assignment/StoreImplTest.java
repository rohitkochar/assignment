package com.inmobi.assignment;

import java.util.ArrayList;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import com.inmobi.exceptions.InitializationException;

public class StoreImplTest {
    
    private Store<String>      store;
    public static final String configFile1 = "src/test/resources/config.properties";
    public static final String configFile2 = "src/test/resources/config1.properties";
    public static final String configFile3 = "src/test/resources/config2.properties";
    
    @AfterTest
    public void close() {
    
        store.destroy();
    }
    
    @Test
    public void testConfigFileMissingValues() throws InitializationException {
    
        store = new StoreImpl<String>();
        store.init(configFile3);
        store.put("hello", "hello".getBytes());
    }
    
    @Test
    public void testGetNonExistentValue() throws InitializationException {
    
        store = new StoreImpl<String>();
        store.init(configFile1);
        
        store.put("testGetNonExistentValue", "testGetNonExistentValue ".getBytes());
        Assert.assertEquals(null, store.get("no key"));
    }
    
    @Test
    public void testLowMaxMemory() throws InitializationException {
    
        store = new StoreImpl<String>();
        store.init(configFile2);
        
        store.put("testLowMaxMemory", "test value".getBytes());
        store.put("testLowMaxMemory key1", "test value1".getBytes());
        store.put("testLowMaxMemory key2", "test value2".getBytes());
        Assert.assertEquals("test value", new String(store.get("testLowMaxMemory")));
    }
    
    @Test(expectedExceptions = InitializationException.class)
    public void testNoInit() throws InitializationException {
    
        store = new StoreImpl<String>();
        store.put("hello", "hi".getBytes());
    }
    
    @Test
    public void testPut() throws InitializationException {
    
        store = new StoreImpl<String>();
        store.init(configFile1);
        
        store.put("testPut key", "test value".getBytes());
        Assert.assertEquals("test value", new String(store.get("testPut key")));
        
    }
    
    @Test
    public void testPutDuplicateValueGetFromPersistentStore() throws InitializationException, InterruptedException {
    
        store = new StoreImpl<String>();
        store.init(configFile2);
        store.put("testPutDuplicateValueGetFromPersistentStore", "test value".getBytes());
        store.put("testPutDuplicateValueGetFromPersistentStore", "test value changed".getBytes());
        Thread.sleep(1500); // drainage time is 1 sec
        Assert.assertEquals("test value changed", new String(store.get("testPutDuplicateValueGetFromPersistentStore")));
        
    }
    
    @Test
    public void testPutDuplicateValueGetImmediately() throws InitializationException {
    
        store = new StoreImpl<String>();
        store.init(configFile1);
        store.put("testPutDuplicateValueGetImmediately", "test value".getBytes());
        store.put("testPutDuplicateValueGetImmediately", "test value changed".getBytes());
        
        Assert.assertEquals("test value changed", new String(store.get("testPutDuplicateValueGetImmediately")));
        
    }
    
    @Test
    public void testPutLotOfRandomValues() throws InitializationException {
    
        store = new StoreImpl<String>();
        store.init(configFile1);
        ArrayList<Integer> list = new ArrayList<Integer>();
        Random rand = new Random();
        int tmp1 = 0;
        for (int i = 0; i <= 50; i++) {
            tmp1 = rand.nextInt(1000);
            list.add(tmp1);
            store.put(Integer.toString(tmp1), Integer.toString(tmp1).getBytes());
        }
        store.get(Integer.toString(tmp1));
        Random r = new Random();
        for (int i = 0; i < 50; i++) {
            int tmp = r.nextInt(1000);
            if (list.contains(tmp)) {
                Assert.assertEquals(Integer.toString(tmp), new String(store.get(Integer.toString(tmp))));
            } else {
                Assert.assertEquals(null, store.get(Integer.toString(tmp)));
            }
        }
        
    }
    
    @Test(expectedExceptions = InitializationException.class)
    public void testWrongConfigFilePath() throws InitializationException {
    
        store = new StoreImpl<String>();
        store.init("abc");
    }
}
