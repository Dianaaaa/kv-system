package com.kvstore.node.impl;

import com.kvstore.node.NodeService;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NodeServiceImpl extends UnicastRemoteObject implements NodeService {
    Map store;
    Map<String, Lock> locks;
    Lock globalLock;

    public NodeServiceImpl()  throws RemoteException{
        store = new HashMap<String, String>();
        locks = new HashMap<String, Lock>();
        globalLock = new ReentrantLock();
    }

    public void init(Map initValues) throws RemoteException {
        System.out.println("init node: "+ initValues.toString());
        store = new HashMap(initValues);
        Set<String> keySet = store.keySet();
        for (String key : keySet) {
            locks.put(key, new ReentrantLock());
        }
    }

    public String getData(String key) throws RemoteException {
        if (locks.containsKey(key)) {
            Lock lock = locks.get(key);
            lock.lock();
            System.out.println("getData: " + key);
            String result = store.get(key).toString();
            lock.unlock();
            return result;
        } else {
            globalLock.lock();
            System.out.println("getData: " + key);
            String result = store.get(key).toString();
            globalLock.unlock();
            return result;
        }
    }
    public void putData(String key, String value) throws RemoteException {
        if (locks.containsKey(key)) {
            Lock lock = locks.get(key);
            lock.lock();
            System.out.println("PutData: " + key + ", " + value);
            store.put(key, value);
            lock.unlock();
        } else {
            globalLock.lock();
            System.out.println("PutData: " + key + ", " + value);
            store.put(key, value);
            locks.put(key, new ReentrantLock());
            globalLock.unlock();
        }
    }
    public void deleteData(String key) throws RemoteException {
        if (locks.containsKey(key)) {
            Lock lock = locks.get(key);
            lock.lock();
            System.out.println("deleteData: " + key);
            store.remove(key);
            lock.unlock();
        } else {
            globalLock.lock();
            System.out.println("deleteData: " + key);
            store.remove(key);
            globalLock.unlock();
        }
    }
}
