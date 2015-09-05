package me.chunyu.rpc_proxy;

import me.chunyu.rpc_proxy.server.TNonblockingServer;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class RpcServerBase extends TNonblockingServer {
    protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());
    protected String serviceName;
    protected String productName;
    protected ConfigFile config;

    public RpcServerBase(TProcessor processor, String configPath) {
        super(processor);
        config = new ConfigFile(configPath);

        this.serviceName = config.service;
        this.productName = config.productName;

        setUp(config.workers, 5000);
    }


    @Override
    public void serve() {
        TNonblockingServerSocket socket;

        try {
            InetSocketAddress address = new InetSocketAddress(config.frontendAddr, config.frontPort);
            socket = new TNonblockingServerSocket(address);
        } catch (Exception e) {
            throw new RuntimeException("");
        }
        setServerTransport(socket);
        super.serve();
    }
}
