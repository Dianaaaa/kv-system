package com.kvstore.node.impl;

import com.kvstore.node.NodeService;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

public class NodeServiceImpl extends UnicastRemoteObject implements NodeService {
    Map store;

    public NodeServiceImpl()  throws RemoteException{
        store = new HashMap<String, String>();
    }

    public void init(Map initValues) throws RemoteException {
        store = new HashMap(initValues);
    }

    public String getData(String key) throws RemoteException {
        System.out.println("getData: " + key);
        return store.get(key).toString();
    }
    public void putData(String key, String value) throws RemoteException {
        System.out.println("PutData: " + key + ", " + value);
        store.put(key, value);
    }
    public void deleteData(String key) throws RemoteException {
        System.out.println("deleteData: " + key);
        store.remove(key);
    }
}
