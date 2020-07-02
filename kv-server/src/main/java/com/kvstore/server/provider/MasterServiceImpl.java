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
import java.util.Set;

public class MasterServiceImpl extends UnicastRemoteObject implements MasterService {

    Map kvs;
    public String[] rmiPaths;
    public Integer normalNodeNum = 0;
    public Integer backupNodeNum = 0;
    public String[] backupRmiPaths;

    public MasterServiceImpl() throws RemoteException {
        //init();
    }

    public void init() throws RemoteException {
        System.out.println("Master init");
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

//            if (this.nodeNum != 0) {
//                Set<String> keySet = kvs.keySet();
//                for (String) {
//
//                }
//            }
        } catch (java.io.FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public void distributeKeys() {
        System.out.println("key distributing");
        if (this.normalNodeNum != 0) {
            for (int i = 0; i < normalNodeNum; i++) {
                try {
                    NodeService node = (NodeService) Naming.lookup(rmiPaths[i]);
                    node.init(new HashMap<String, String>());
                    // init backup node
                    if (i < backupNodeNum) {
                        NodeService backupNode = (NodeService) Naming.lookup(backupRmiPaths[i]);
                        backupNode.init(new HashMap<String, String>());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Set<String> keySet = kvs.keySet();
            for (String key : keySet) {
                String value = kvs.get(key).toString();
                Integer index = key.hashCode() % normalNodeNum;
                try {
                    System.out.println("["+key+","+value+"]"+rmiPaths[index]);
                    NodeService node = (NodeService) Naming.lookup(rmiPaths[index]);
                    node.putData(key, value);

                    // put data to backup node
                    if (index < backupNodeNum) {
                        System.out.println("["+key+","+value+"]"+backupRmiPaths[index]);
                        NodeService backupNode = (NodeService) Naming.lookup(backupRmiPaths[index]);
                        backupNode.putData(key, value);
                    }
                } catch (NotBoundException e) {
                    e.printStackTrace();
                } catch (RemoteException e) {
                    e.printStackTrace();
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void PUT(String key, String value) throws RemoteException {
        System.out.println("Put: " + key + ", " + value);
        kvs.put(key, value);
        Integer index = key.hashCode() % normalNodeNum;
        try {
            NodeService node = (NodeService) Naming.lookup(rmiPaths[index]);
            node.putData(key, value);
        } catch (Exception e) {
            e.printStackTrace();
            if (index < backupNodeNum) {
                // this node has backup
                rmiPaths[index] = backupRmiPaths[index];
                PUT(key, value);
            }
            return;

        }
        // also update backup node
        if (index < backupNodeNum) {
            try {
                NodeService node = (NodeService) Naming.lookup(backupRmiPaths[index]);
                node.putData(key, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println(kvs);
    }

    public String READ(String key) throws RemoteException {
        System.out.println("Read: " + key);
        String result = null;
        Integer index = key.hashCode() % normalNodeNum;
        try {
            NodeService node = (NodeService) Naming.lookup(rmiPaths[index]);
            result =  node.getData(key);
        } catch (Exception e) {
            e.printStackTrace();
            if (index < backupNodeNum) {
                // this node has backup
                rmiPaths[index] = backupRmiPaths[index];
                return READ(key);
            }

        }
        return result;
    }

    public void DELETE(String key) throws RemoteException {
        System.out.println("Delete: " + key);
        kvs.remove(key);
        Integer index = key.hashCode() % normalNodeNum;
        try {
            NodeService node = (NodeService) Naming.lookup(rmiPaths[index]);
            node.deleteData(key);
        } catch (Exception e) {
            e.printStackTrace();
            if (index < backupNodeNum) {
                // this node has backup
                rmiPaths[index] = backupRmiPaths[index];
                DELETE(key);
                return;
            }

        }
        // also update backup node
        if (index < backupNodeNum) {
            try {
                NodeService node = (NodeService) Naming.lookup(backupRmiPaths[index]);
                node.deleteData(key);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
