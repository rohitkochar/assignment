package com.inmobi.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.log4j.Logger;

class BSTNode<K extends Serializable & Comparable<K>> implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    public static <T extends Serializable & Comparable<T>> BSTNode<T> getNewNode(T key, byte[] value) {
    
        BSTNode<T> node = new BSTNode<T>();
        node.setKey(key);
        node.setValue(value);
        node.setLeftOffset(-1);
        node.setRightOffset(-1);
        return node;
    }
    
    private K      key;
    private byte[] value;
    private long   leftOffset;
    
    private long   rightOffset;
    
    public K getKey() {
    
        return key;
    }
    
    public long getLeftOffset() {
    
        return leftOffset;
    }
    
    public long getRightOffset() {
    
        return rightOffset;
    }
    
    public byte[] getValue() {
    
        return value;
    }
    
    public void setKey(K key) {
    
        this.key = key;
    }
    
    public void setLeftOffset(long leftOffset) {
    
        this.leftOffset = leftOffset;
    }
    
    public void setRightOffset(long rightOffset) {
    
        this.rightOffset = rightOffset;
    }
    
    public void setValue(byte[] value) {
    
        this.value = value;
    }
    
}

/**
 * This class is used to store data in a file
 * Data is stored in the form of binary search tree to aid effective searching
 * Keys are assumed to be comparable
 * 
 */
public class TreeFileStorageHandler<K extends Serializable & Comparable<K>> implements StorageHandler<K> {
    
    private final String     filePath;
    private final Logger     logger = Logger.getLogger(this.getClass());
    
    private RandomAccessFile file;
    
    public TreeFileStorageHandler(String filePath) {
    
        this.filePath = filePath;
        try {
            logger.debug("File path of persistent store is " + filePath);
            File tmpFile = new File(filePath);
            if (tmpFile.exists()) {
                logger.debug("File already existed hence deleting and creating a new file");
                tmpFile.delete();
            }
            file = new RandomAccessFile(tmpFile, "rw");
        } catch (FileNotFoundException e) {
            logger.error("Storage file not found", e);
        }
    }
    
    public void deleteAll() {
    
        File file = new File(filePath);
        
        try {
            new FileOutputStream(file).write((new String()).getBytes());
        } catch (IOException e) {
            logger.error("Error while creating storage file", e);
        }
        
    }
    
    public byte[] get(K key) {
    
        logger.debug("Getting key =" + key + " from persistent store");
        try {
            if (file.length() == 0) {
                logger.debug("File is empty");
                return null;
            } else {
                BSTNode<K> currentNode = getNode(0);
                long currentOffset = 0;
                while ((currentOffset != -1) && !currentNode.getKey().equals(key)) {
                    if (key.compareTo(currentNode.getKey()) < 0) {
                        currentOffset = currentNode.getLeftOffset();
                        if (currentOffset != -1) {
                            currentNode = getNode(currentOffset);
                        }
                    } else {
                        currentOffset = currentNode.getRightOffset();
                        if (currentOffset != -1) {
                            currentNode = getNode(currentOffset);
                        }
                    }
                }
                if (currentOffset == -1) return null;
                else
                    return currentNode.getValue();
            }
        } catch (IOException e) {
            logger.error("Error while converting bytes to BSTNode", e);
        }
        return null;
        
    }
    
    private byte[] getBytes(Serializable s) {
    
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream dout;
        try {
            dout = new ObjectOutputStream(bout);
            
            dout.writeObject(s);
        } catch (IOException e) {
            logger.error("Object " + s + "Cannot be serialized");
        }
        return bout.toByteArray();
        
    }
    
    public Iterator<K> getIterator() {
    
        throw new UnsupportedOperationException();
    }
    
    private BSTNode<K> getNode(long filePosition) throws IOException {
    
        file.seek(filePosition);
        int sizeOfRootNode = file.readInt();
        byte b[] = new byte[sizeOfRootNode];
        file.read(b);
        BSTNode<K> node = getObject(b);
        return node;
    }
    
    @SuppressWarnings("unchecked")
    private BSTNode<K> getObject(byte[] b) {
    
        ByteArrayInputStream bin = new ByteArrayInputStream(b);
        ObjectInputStream din;
        BSTNode<K> node = null;
        try {
            din = new ObjectInputStream(bin);
            node = (BSTNode<K>) din.readObject();
        } catch (Exception e) {
            logger.error("Corrupted data node retrieved from file", e);
        }
        return node;
    }
    
    private void insertIntoBST(K key, byte[] value) throws IOException {
    
        BSTNode<K> node = BSTNode.getNewNode(key, value);
        
        if (file.length() == 0) {// file is empty;insert node at start
            insertNodeAt(0, node);
        } else {
            BSTNode<K> currentNode = getNode(0);
            long currentOffset = 0;
            long previousOffset = -1;
            boolean isLeftChild = false;
            long newNodeOffset = -1;
            while (currentOffset != -1) {
                logger.debug("Traversing the tree..currently at node with kye =" + currentNode.getKey());
                if (key.compareTo(currentNode.getKey()) == 0) {
                    // key already exist hence overwrite the
                    // node but the new node should have
                    // same left and right child as before
                    node.setLeftOffset(currentNode.getLeftOffset());
                    node.setRightOffset(currentNode.getRightOffset());
                    insertNodeAt(currentOffset, node);
                    break; // end the loop as update done
                } else {
                    if (key.compareTo(currentNode.getKey()) < 0) {
                        previousOffset = currentOffset;
                        currentOffset = currentNode.getLeftOffset();
                        isLeftChild = true;
                        newNodeOffset = file.length();
                    } else if (key.compareTo(currentNode.getKey()) > 0) {
                        previousOffset = currentOffset;
                        currentOffset = currentNode.getRightOffset();
                        isLeftChild = false;
                        newNodeOffset = file.length();
                    }
                    if (currentOffset != -1) {
                        currentNode = getNode(currentOffset);
                    } else {// we are at leaf node hence insert here and update
                            // the currentNode
                        newNodeOffset = file.length();
                        if (isLeftChild) {
                            currentNode.setLeftOffset(newNodeOffset);
                        } else {
                            currentNode.setRightOffset(newNodeOffset);
                        }
                        
                        // updating the parent node to point to new child's
                        // offset
                        insertNodeAt(previousOffset, currentNode);
                        // inserting the new node
                        logger.debug("Adding new node with key=" + key + " as the child of " + currentNode.getKey()
                                        + " at offset " + newNodeOffset);
                        insertNodeAt(newNodeOffset, node);
                    }
                }
            }
        }
    }
    
    private void insertNodeAt(long offset, BSTNode<K> node) throws IOException {
    
        file.seek(offset);
        byte nodeBytes[] = getBytes(node);
        file.writeInt(nodeBytes.length);
        file.write(nodeBytes);
    }
    
    public boolean isFull() {
    
        return false;
    }
    
    public void put(K key, byte[] value) {
    
        try {
            logger.debug("Adding key =" + key + "in file");
            insertIntoBST(key, value);
        } catch (IOException e) {
            logger.error("Error while writing key value to file for key =" + key, e);
            
        }
        
    }
    
}
