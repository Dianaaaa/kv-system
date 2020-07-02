package com.kvstore.node.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import sun.applet.AppletViewerPanel;

import java.util.concurrent.CountDownLatch;

public class ZKConnectionWatcher implements Watcher {
    public static ZooKeeper zooKeeper;
    public static CountDownLatch countDownLatch = new CountDownLatch(1);

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
                            zooKeeper = new ZooKeeper("127.0.0.1:2181", 5000, new ZKConnectionWatcher());
                        } else if (watchedEvent.getState() == Event.KeeperState.AuthFailed) {
                            System.out.println("auth failed");
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

    }

    public static void main(String[] args) {
        try {
            zooKeeper = new ZooKeeper("127.0.0.1:2189", 500, new ZKConnectionWatcher());
            countDownLatch.await();
            System.out.println(zooKeeper.getSessionId());
            zooKeeper.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
