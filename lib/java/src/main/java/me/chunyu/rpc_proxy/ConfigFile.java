package me.chunyu.rpc_proxy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class ConfigFile {
    protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());
    String productName;
    String service;

    String zkAddr;
    int zkSessionTimeout;

    int workers;

    private String frontHost;
    int frontPort;
    String ipPrefix;
    boolean verbose;


    public String getFrontHost() {
        if (StringUtils.isEmpty(this.frontHost)) {
            try {
                for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
                    NetworkInterface intf = en.nextElement();
                    for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements(); ) {
                        String ipAddr = enumIpAddr.nextElement().getHostAddress();
                        if (ipAddr.startsWith(ipPrefix)) {
                            this.frontHost = ipAddr;
                            break;
                        }
                    }
                }
            } catch (SocketException e) {
                LOGGER.info(" (error retrieving network interface list)");
            }
        }
        return this.frontHost;
    }

    public ConfigFile(String path) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path)), "utf-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if ((line.indexOf("#") != -1) || (line.length() == 0)) {
                    continue;
                }
                String[] fields = line.split("=");
                if (fields.length != 2) {
                    continue;
                }

                String key = fields[0].trim();
                String value = fields[1].trim();

                if ("zk".equals(key)) {
                    zkAddr = value;
                } else if ("product".equals(key)) {
                    productName = value;
                } else if ("verbose".equals(key)) {
                    verbose = value.equals("1");
                } else if ("service".equals(key)) {
                    service = value;
                } else if ("zk_session_timeout".equals(key)) {
                    zkSessionTimeout = Integer.parseInt(value) * 1000;
                } else if ("front_host".equals(key)) {
                    frontHost = value;
                } else if ("front_port".equals(key)) {
                    frontPort = Integer.parseInt(value);
                } else if ("ip_prefix".equals(key)) {
                    ipPrefix = value;
                } else if ("workers".equals(key)) {
                    workers = Integer.parseInt(value);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("");
        }
    }
}
