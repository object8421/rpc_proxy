package me.chunyu.rpc_proxy;


import org.junit.Test;

import java.net.URL;

public class TestConfigFile {
    @Test
    public void testConfigFileParse() throws Exception {

        URL url = getClass().getResource("/config.ini");
        String filePath = url.getFile();

        ConfigFile file = new ConfigFile(filePath);

        file.getFrontHost();

    }
}
