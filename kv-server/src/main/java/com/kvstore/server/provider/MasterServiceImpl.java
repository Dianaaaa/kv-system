package com.kvstore.server.provider;

import com.kvstore.node.NodeService;
import com.kvstore.server.MasterService;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class MasterServiceImpl extends UnicastRemoteObject implements MasterService {

    Map kvs;

    public MasterServiceImpl() throws RemoteException {
        init();
    }

    public void init() throws RemoteException {
        kvs = new HashMap<String, String>();
        File file = new File("src/main/resources/data.txt");
        if(!file.exists()){
            return;
        }
        try {
            Scanner scanner=new Scanner(file);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                System.out.println(line);
                String[] lineSplit=line.split(" ");
                kvs.put(lineSplit[0], lineSplit[1]);
            }
        } catch (java.io.FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public void PUT(String key, String value) throws RemoteException {
        System.out.println("Put: " + key + ", " + value);
        kvs.put(key, value);
        try {
            NodeService node = (NodeService) Naming.lookup("rmi://localhost:1099/kvNode");
            node.putData(key, value);
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        System.out.println(kvs);
    }

    public String READ(String key) throws RemoteException {
        System.out.println("Read: " + key);
        String result = null;
        try {
            NodeService node = (NodeService) Naming.lookup("rmi://localhost:1099/kvNode");
            result =  node.getData(key);
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public void DELETE(String key) throws RemoteException {
        System.out.println("Delete: " + key);
        //kvs.remove(key);
        try {
            NodeService node = (NodeService) Naming.lookup("rmi://localhost:1099/kvNode");
            node.deleteData(key);
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

}
