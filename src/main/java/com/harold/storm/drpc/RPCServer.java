package com.harold.storm.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class RPCServer {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        RPC.Builder builder = new RPC.Builder(conf);
        RPC.Server server = builder.setProtocol(UserService.class)
                .setInstance(new userServiceImpl())
                .setBindAddress("localhost")
                .setPort(9999)
                .build();
        server.start();
    }
}
