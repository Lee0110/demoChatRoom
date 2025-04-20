package com.lyl.demoChatRoom;

import com.lyl.demoChatRoom.netty.NettyWebSocketServer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoChatRoomApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(DemoChatRoomApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        // 启动Netty服务（端口8081）
        new NettyWebSocketServer(8081).start();
    }
}
