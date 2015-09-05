package me.chunyu.rpc_proxy;

import me.chunyu.rpc_proxy.server.TNonblockingServer;
import me.chunyu.rpc_proxy.zk.CuratorRegister;
import me.chunyu.rpc_proxy.zk.ServiceEndpoint;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.net.InetSocketAddress;

public class GeneralRpcServer extends TNonblockingServer {
    protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());
    protected String serviceName;
    protected String productName;
    protected ConfigFile config;

    protected ServiceEndpoint endpoint;
    protected CuratorRegister curator;

    public GeneralRpcServer(TProcessor processor, String configPath) {
        super(processor);
        config = new ConfigFile(configPath);

        this.serviceName = config.service;
        this.productName = config.productName;

        if (config.workers < 1) {
            throw new RuntimeException("Invalid Worker Number");
        }
        setUp(config.workers, 5000);

        String hostport = String.format("%s:%d", config.getFrontHost(), config.frontPort);
        String serviceId = ServiceEndpoint.getServiceId(hostport);
        endpoint = new ServiceEndpoint(productName, serviceName, serviceId, hostport);
        curator = new CuratorRegister(config.zkAddr);
        curator.getCurator().start();
    }


    @Override
    public void serve() {
        TNonblockingServerSocket socket;

        try {
            InetSocketAddress address = new InetSocketAddress(config.getFrontHost(), config.frontPort);
            socket = new TNonblockingServerSocket(address);
        } catch (Exception e) {
            LOGGER.error("Socket Error: ", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
        setServerTransport(socket);
        super.serve();
    }

    @Override
    protected void setServing(boolean serving) {
        super.setServing(serving);
        try {
            if (serving) {
                endpoint.addServiceEndpoint(curator);
            } else {
                endpoint.deleteServiceEndpoint(curator);
            }
        } catch(Exception e) {
            LOGGER.error("Service Register Error", e);
        }
    }
}
