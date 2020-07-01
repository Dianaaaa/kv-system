package com.kvstore.node.impl;

import com.kvstore.node.NodeService;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public class Node {
    public static void main(String[] args) {
        try {
            // 本地主机上的远程对象注册表Registry的实例
            LocateRegistry.createRegistry(1099);
            // 创建一个远程对象
            NodeService node = new NodeServiceImpl();
            // 把远程对象注册到RMI注册服务器上，并命名为Hello
            //绑定的URL标准格式为：rmi://host:port/name
            Naming.bind("rmi://localhost:1099/kvNode", node);
            System.out.println("======= node启动RMI服务成功! =======");
            System.in.read();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        }

    }
}
