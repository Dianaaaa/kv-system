# Key-Value Storage System

516072910066 钱星月

# 一、总体设计：

​	![总体设计](./img/总体设计.png)

Client：用户角色，负责发送PUT、READ、DELETE请求。

Master：负责初始化整个存储集群，负责分发来自Client的请求，负责在node数量变化时管理集群做出相应对策

Node：负责存储数据，负责新增、查询、删除数据的任务。

Backup：Node的备份，当node出错时Master将指定它们替代node，成为新的node。

## 二、开发过程

采用增量式开发，从简单到复杂

### 第一步 构建一个简单的Client --> Master --> Node的结构：

![version1](./img/version1.png)

使用Java的RMI发送RPC。现在本地试验，熟悉RMI的用法。

Master接口：

```java
public interface MasterService extends Remote {
    public void PUT(String key, String value) throws RemoteException;

    public String READ(String key) throws RemoteException;

    public void DELETE(String key) throws RemoteException;
}
```

Node接口：

```java
public interface NodeService extends Remote {
    void init(Map initValues) throws RemoteException;
    String getData(String key) throws RemoteException;
    void putData(String key, String value) throws RemoteException;
    void deleteData(String key) throws RemoteException;
}
```

Master接口实现方式：

```java
public void PUT(String key, String value) throws RemoteException {
        NodeService node = (NodeService) Naming.lookup("rmi://localhost:1099/kvNode");
        node.putData(key, value);
    }

    public String READ(String key) throws RemoteException {
        NodeService node = (NodeService) Naming.lookup("rmi://localhost:1099/kvNode");
        result =  node.getData(key);
        return result;
    }

    public void DELETE(String key) throws RemoteException {
        NodeService node = (NodeService) Naming.lookup("rmi://localhost:1099/kvNode");
        node.deleteData(key);
    }
```

最后client通过远程调用MasterService的函数，向Node节点写入、读取、删除数据。

### 第二步 加入Zookeeper管理RPC服务绑定的地址

由于地址是在代码中写死的，现在我们用zookeeper来管理。