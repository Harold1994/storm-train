package com.harold.storm.drpc;

public class userServiceImpl implements UserService {
    public void addUser(String name, int age) {
        System.out.println("Adding new user,name: " + name);
    }
}
