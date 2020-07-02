package com.kvstore.server.provider;

import com.kvstore.server.MasterService;
import com.kvstore.server.watcher.ZKConnectionWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import static com.kvstore.server.watcher.ZKConnectionWatcher.countDownLatch;

public class Master {
    static ZooKeeper zooKeeper;

    public static void main(String[] args) {

        try {
            // 本地主机上的远程对象注册表Registry的实例
            LocateRegistry.createRegistry(1100);
            // 创建一个远程对象
            MasterService master = new MasterServiceImpl();
            // 把远程对象注册到RMI注册服务器上，并命名为Hello
            //绑定的URL标准格式为：rmi://host:port/name
            Naming.bind("rmi://localhost:1100/kvMaster", master);

            System.out.println("======= master启动RMI服务成功! =======");

            zooKeeper = new ZooKeeper("172.20.10.2:2181", 500, new ZKConnectionWatcher((MasterServiceImpl)master));
            countDownLatch.await();
            System.out.println(zooKeeper.getSessionId());

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
