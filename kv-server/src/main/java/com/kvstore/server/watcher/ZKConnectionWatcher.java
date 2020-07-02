package com.kvstore.server.watcher;

import com.kvstore.server.provider.MasterServiceImpl;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import sun.applet.AppletViewerPanel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ZKConnectionWatcher implements Watcher {
    public static ZooKeeper zooKeeper;
    public static CountDownLatch countDownLatch = new CountDownLatch(1);

    MasterServiceImpl master;
    String[] rmiPaths;
    Integer nodeNum = 0;

    public void initValue() {
        try {
            zooKeeper = new ZooKeeper("172.20.10.2:2181", 500, this);
            countDownLatch.await();

            zooKeeper.setData("/config/nodeNum", "0".getBytes(), -1);
            zooKeeper.setData("/config/nodeService", "".getBytes(), -1);

            byte[] nodeNum = zooKeeper.getData("/config/nodeNum", false, null);

            this.nodeNum = Integer.valueOf(new String(nodeNum)).intValue();
            String nodeServer = new String(zooKeeper.getData("/config/nodeService", true, null));
            rmiPaths = nodeServer.split(" ");
            System.out.println("nodeNum: "+ this.nodeNum);
            System.out.println("rmiPaths: ");
            for(int i=0;i<this.rmiPaths.length;i++)
            {
                System.out.println(this.rmiPaths[i]);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ZKConnectionWatcher(MasterServiceImpl masterService) {
        this.master = masterService;
        initValue();
        masterInit();
    }

    public void masterInit() {
        this.master.nodeNum = this.nodeNum;
        this.master.rmiPaths = this.rmiPaths;
        try {
            this.master.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent watchedEvent) {
        try {
            if (watchedEvent.getType() == Event.EventType.None) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("connected successfully.");
                    countDownLatch.countDown();
                } else if (watchedEvent.getState() == Event.KeeperState.Disconnected) {
                    System.out.println("connect failed");
                } else if (watchedEvent.getState() == Event.KeeperState.Expired) {
                    System.out.println("time out");
                } else if (watchedEvent.getState() == Event.KeeperState.AuthFailed) {
                    System.out.println("auth failed");
                }
            } else if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
                System.out.println("Node Data changed");
                byte[] nodeNum = zooKeeper.getData("/config/nodeNum", false, null);
                this.nodeNum = Integer.valueOf(new String(nodeNum)).intValue();
                String nodeServer = new String(zooKeeper.getData("/config/nodeService", true, null));
                rmiPaths = nodeServer.split(" ");
                System.out.println("nodeNum: "+ this.nodeNum);
                System.out.println("rmiPaths: ");
                for(int i=0;i<this.rmiPaths.length;i++)
                {
                    System.out.println(this.rmiPaths[i]);
                }
                this.master.nodeNum = this.nodeNum;
                this.master.rmiPaths = this.rmiPaths;
                this.master.distributeKeys();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
