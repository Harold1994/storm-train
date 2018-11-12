package com.harold.storm.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class RPCCLient {
    public static void main(String[] args) throws IOException {
        long versionID = 88888888;
        UserService userService = RPC.getProxy(UserService.class, versionID, new InetSocketAddress("localhost",9999), new Configuration());
        userService.addUser("lisi", 18);
        System.out.println("from client invoked");
        RPC.stopProxy(userService);
    }
}
