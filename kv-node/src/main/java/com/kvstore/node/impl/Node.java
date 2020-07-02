package com.kvstore.node.impl;

import com.kvstore.node.NodeService;
import com.kvstore.node.watcher.ZKConnectionWatcher;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.concurrent.CountDownLatch;

import static com.kvstore.node.watcher.ZKConnectionWatcher.countDownLatch;

public class Node {
    static ZooKeeper zooKeeper;
    static String bindPath = "rmi://172.20.10.2:1099/kvNode";

    public static void addNodeNum() {
        try {
            Stat stat = new Stat();
            byte[] nodeNum = zooKeeper.getData("/config/nodeNum", false, stat);
            Integer num = Integer.valueOf(new String(nodeNum)).intValue();
            num++;
            zooKeeper.setData("/config/nodeNum", num.toString().getBytes(), -1);
            System.out.println("set nodeNum: " + num);

            byte[] paths = zooKeeper.getData("/config/nodeService", false, stat);
            String pathsStr = new String(paths);
            String newPaths;

            if (num == 1) {
                newPaths = bindPath;
            } else {
                newPaths = new String(paths) + " " + bindPath;
            }
            zooKeeper.setData("/config/nodeService", newPaths.getBytes(), -1);
            System.out.println("set rmiPaths: " + newPaths);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void minusNodeNum() {
        try {
            Stat stat = new Stat();
            byte[] nodeNum = zooKeeper.getData("/config/nodeNum", false, stat);
            Integer num = Integer.valueOf(new String(nodeNum)).intValue();
            num--;
            zooKeeper.setData("/config/nodeNum", num.toString().getBytes(), -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {

        try {
            // 本地主机上的远程对象注册表Registry的实例
            LocateRegistry.createRegistry(1099);
            // 创建一个远程对象
            NodeService node = new NodeServiceImpl();
            // 把远程对象注册到RMI注册服务器上，并命名为Hello
            //绑定的URL标准格式为：rmi://host:port/name

            Naming.bind("kvNode", node);
            System.out.println("======= node启动RMI服务成功! =======");

            //update zookeeper data node
            zooKeeper = new ZooKeeper("127.0.0.1:2181", 500, new ZKConnectionWatcher());
            countDownLatch.await();
            System.out.println(zooKeeper.getSessionId());

            //Integer nodeId = getNodeId();
            //String serviceStr = nodeId + " " + bindPath;
            //zooKeeper.create("/config/nodeService", serviceStr.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            addNodeNum();


            System.in.read();

            zooKeeper.close();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
