package me.chunyu.rpc_proxy;

import me.chunyu.rpc_proxy.server.TNonblockingServer;
import org.apache.thrift.TProcessor;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class RpcServerBase extends TNonblockingServer {

    protected ZooKeeper zk;
    protected String serviceName;
    protected String productName;

    public RpcServerBase(TProcessor processor) {
        super(processor);
    }

    public void setUp() {
        // TODO: 读取配置文件
        super.setUp(5, 5);
    }

    public void registerService2Zookeeper() throws IOException {
        this.zk = new ZooKeeper("127.0.0.1:2181", 30, new Watcher() {
            // 监控所有被触发的事件
            public void process(WatchedEvent event) {
                System.out.println("已经触发了" + event.getType() + "事件！");
            }
        });


    }

}
