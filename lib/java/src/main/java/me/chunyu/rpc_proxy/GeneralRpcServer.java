package me.chunyu.rpc_proxy;

import me.chunyu.rpc_proxy.os.ExitHandler;
import me.chunyu.rpc_proxy.os.ExitSignalListener;
import me.chunyu.rpc_proxy.server.TNonblockingServer;
import me.chunyu.rpc_proxy.zk.CuratorRegister;
import me.chunyu.rpc_proxy.zk.ServiceEndpoint;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.net.InetSocketAddress;

public class GeneralRpcServer extends TNonblockingServer implements ExitSignalListener {
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

        // sort out exit handler
        ExitHandler exit = new ExitHandler();
        exit.addListener(this);
        Signal.handle(new Signal("INT"), exit);
        Signal.handle(new Signal("TERM"), exit);


        String hostport = String.format("%s:%d", config.getFrontHost(), config.frontPort);
        String serviceId = ServiceEndpoint.getServiceId(hostport);
        endpoint = new ServiceEndpoint(productName, serviceName, serviceId, hostport);
        curator = new CuratorRegister(config.zkAddr);
        curator.getCurator().start();
    }


    @Override
    public void notifyExit() {
        stop();
    }

    @Override
    public void stop() {
        // 下线服务(等待LB先将自己下线)
        setServing(false);


        // 然后等待结束服务:
        while (System.currentTimeMillis() - lastRequestTime.get() < 5000) {
            try {
                LOGGER.info("Sleeping 1 seconds");
                Thread.sleep(1000);
            } catch (Exception e) {
                //
            }
        }

        // 如何停止这些服务呢?
        super.stop();
    }

    @Override
    public void serve() {
        // 创建Server Socket
        TNonblockingServerSocket socket;

        try {
            InetSocketAddress address = new InetSocketAddress(config.getFrontHost(), config.frontPort);
            socket = new TNonblockingServerSocket(address);
        } catch (Exception e) {
            LOGGER.error("Socket Error: ", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
        // 设置Server Socket
        setServerTransport(socket);

        // 启动Server
        super.serve();
    }

    @Override
    protected void setServing(boolean serving) {
        if (this.isServing() == serving) {
            return;
        }

        super.setServing(serving);
        try {
            // 在服务启动或停止时注册服务
            // TODO: 如果zk的session过期，如何重新注册呢? 如果出现异常又改如何处理呢?
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
