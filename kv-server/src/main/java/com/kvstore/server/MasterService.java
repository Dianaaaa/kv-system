package com.kvstore.server;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public interface MasterService extends Remote {
    public void PUT(String key, String value) throws RemoteException;

    public String READ(String key) throws RemoteException;

    public void DELETE(String key) throws RemoteException;
}
