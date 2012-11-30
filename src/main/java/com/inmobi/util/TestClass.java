package com.inmobi.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import com.inmobi.assignment.StoreImpl;
import com.inmobi.exceptions.InitializationException;

public class TestClass {
    
    /**
     * @param args
     * @throws IOException
     * @throws InitializationException
     */
    public static void main(String[] args) throws IOException, InitializationException {
    
        // TreeFileStorageHandler<String> storageHandler = new
        // TreeFileStorageHandler<String>();
        StoreImpl<Integer> storageHandler = new StoreImpl<Integer>();
        storageHandler.init("src/main/resources/config.properties");
        // storageHandler.put("rohit", "kochar".getBytes());
        // BSTNode<String> root=storageHandler.getNode(0);
        // System.out.println(root.getKey());
        // storageHandler.put("sid", "agarwal".getBytes());
        // storageHandler.put("chachu", "jain".getBytes());
        // storageHandler.put("abc", "abc".getBytes());
        // storageHandler.put("rmit", "rmit".getBytes());
        
        // root=storageHandler.getNode(0);
        // System.out.println(root.getKey());
        // System.out.println(root.getRightOffset());
        // System.out.println(root.getLeftOffset());
        // BSTNode<String>
        // firstNode=storageHandler.getNode(root.getLeftOffset());
        // System.out.println(firstNode.getKey());
        // System.out.println(new String(storageHandler.get("sid")));
        ArrayList<Integer> list = new ArrayList<Integer>();
        Random rand = new Random();
        int tmp1 = 0;
        for (int i = 0; i <= 1000; i++) {
            tmp1 = rand.nextInt(1000);
            list.add(tmp1);
            storageHandler.put(tmp1, new Integer(tmp1).toString().getBytes());
        }
        storageHandler.get(tmp1);
        Random r = new Random();
        for (int i = 0; i < 100; i++) {
            int tmp = r.nextInt(1000);
            byte b[] = storageHandler.get(tmp);
            if (list.contains(tmp)) {
                System.out.println("Fetched value for key =" + tmp + "and value is= " + new String(b));
            } else {
                System.out.println("Fetched value for key =" + tmp + "and value is=" + b);
            }
        }
        storageHandler.destroy();
        
    }
    
}
