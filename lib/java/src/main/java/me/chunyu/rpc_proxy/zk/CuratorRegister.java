package me.chunyu.rpc_proxy.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;

public class CuratorRegister {

    String connectionString;
    CuratorFramework curator;
    public CuratorRegister(String connectionString) {
        this.connectionString = connectionString;
        this.curator = createSimple(this.connectionString);
    }

    public CuratorFramework getCurator() {
        return this.curator;
    }

    protected static CuratorFramework createSimple(String connectionString) {
        // these are reasonable arguments for the ExponentialBackoffRetry.
        // The first retry will wait 1 second - the second will wait up to 2 seconds - the
        // third will wait up to 4 seconds.
        BoundedExponentialBackoffRetry retryPolicy = new BoundedExponentialBackoffRetry(1000, 4000, 3);
        return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    }
    protected static CuratorFramework createWithOptions(String connectionString, RetryPolicy retryPolicy,
                                                     int connectionTimeoutMs, int sessionTimeoutMs) {
        return CuratorFrameworkFactory.builder().connectString(connectionString)
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                        // etc. etc.
                .build();
    }

}
