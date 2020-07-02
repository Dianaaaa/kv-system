package com.kvstore.node;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

public interface NodeService extends Remote {
    void init(Map initValues) throws RemoteException;
    String getData(String key) throws RemoteException;
    void putData(String key, String value) throws RemoteException;
    void deleteData(String key) throws RemoteException;
}