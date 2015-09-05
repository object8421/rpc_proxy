//package me.chunyu.rpc_proxy.zk;
//
//import org.apache.zookeeper.*;
//import org.apache.zookeeper.data.Stat;
//
//import java.io.IOException;
//import java.nio.file.Path;
//
//
//public class ZkRegister {
//    protected ZooKeeper zookeeper;
//    private String zkHosts;
//
//    private ServiceEndpoint endpoint;
//
//    public ZkRegister(String zkHosts, ServiceEndpoint endpoint) {
//        this.zkHosts = zkHosts;
//        this.endpoint = endpoint;
//
//        getZookeeper();
//    }
//
//    private ZooKeeper buildClient() {
//        try {
//            return new ZooKeeper(zkHosts, 30000, new SessionWatcher());
//        } catch (IOException e) {
//            throw new RuntimeException("init zookeeper fail.", e);
//        }
//    }
//
//    public void close() {
//        System.out.println("[SUC-CORE] close");
//        if (zookeeper != null) {
//            try {
//                zookeeper.close();
//                zookeeper = null;
//            } catch (InterruptedException e) {
//                //ignore exception
//            }
//        }
//    }
//
//    public synchronized ZooKeeper getZookeeper() {
//        if (zookeeper == null) {
//            zookeeper = buildClient();
//        }
//        return zookeeper;
//    }
//
//    /**
//     * 递归创建Path
//     *
//     * @param path
//     * @param zk
//     * @throws KeeperException
//     * @throws InterruptedException
//     */
//    public static void createParentDirRecursively(String path, ZooKeeper zk) throws KeeperException, InterruptedException {
//        String[] paths = path.split("/");
//        StringBuilder build = new StringBuilder();
//        for (int i = 0; i < paths.length - 1; i++) {
//            String pathElement = paths[i];
//            build.append("/").append(pathElement);
//            String pathString = path.toString();
//            Stat stat = zk.exists(pathString, false);
//            if (stat != null) {
//                zk.create(pathString, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//            }
//        }
//    }
//
//    protected class SessionWatcher implements Watcher {
//        @Override
//        public void process(WatchedEvent event) {
//            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
//
//            } else if (event.getState() == Watcher.Event.KeeperState.Expired) {
//                System.out.println("[SUC-CORE] session expired. now rebuilding");
//
//                close();
//
//                getZookeeper();
//            }
//        }
//    }
//}
