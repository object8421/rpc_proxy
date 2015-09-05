package me.chunyu.rpc_proxy.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public class ServiceEndpoint {
    String productName;
    String serviceName;
    String serviceId;
    String endpoint;
    String endpointPath;
    byte[] data;

    public static String getServiceId(String hostport) {
        String result = hostport.replace(".", "_").replace("/", "").replace(":", "_");
        if (result.length() > 20) {
            result = result.substring(result.length() - 20);
        }
        return result;
    }

    public ServiceEndpoint(String productName, String serviceName, String serviceId, String endpoint) {
        this.productName = productName;
        this.serviceName = serviceName;
        this.serviceId = serviceId;
        this.endpoint = endpoint;

        String jsonData = String.format("{\"service\": \"%s\", \"service_id\": \"%s\", \"frontend\":\"%s\"}", this.serviceName, this.serviceId, this.endpoint);
        try {
            data = jsonData.getBytes("utf-8");
        } catch (Exception e) {

        }
        endpointPath = String.format("/zk/product/%s/services/%s/%s", this.productName, this.serviceName, this.serviceId);
    }

    public void addServiceEndpoint(CuratorRegister curator) throws Exception {
        Stat stat = curator.getCurator().checkExists().forPath(endpointPath);
        if (stat != null) {
            this.deleteServiceEndpoint(curator);
        }

        // /zk/product/test/services
        curator.getCurator().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(endpointPath, this.data);
    }

    public void deleteServiceEndpoint(CuratorRegister curator) throws Exception {
        Stat stat = curator.getCurator().checkExists().forPath(endpointPath);
        if (stat != null) {
            curator.getCurator().delete().forPath(endpointPath);
        }
    }
}
