package com.lyl.demoChatRoom.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateHandler;

public class WebSocketServerInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        // 处理HTTP请求和WebSocket握手
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpObjectAggregator(65536));
        
        // 自定义跨域处理器
        pipeline.addLast(new CorsHandler());

        pipeline.addLast(new IdleStateHandler(0, 0, 30)); // 30秒无心跳则断开
        
        // WebSocket协议处理器
        pipeline.addLast(new WebSocketServerProtocolHandler("/chat"));
        
        // 自定义业务逻辑处理器
        pipeline.addLast(new ChatHandler());
    }
}