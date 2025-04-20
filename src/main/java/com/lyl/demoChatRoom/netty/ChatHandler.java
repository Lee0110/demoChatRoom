package com.lyl.demoChatRoom.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lyl.demoChatRoom.entity.ChatMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.GlobalEventExecutor;

public class ChatHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {
    // 使用ChannelGroup管理所有连接
    private static final ChannelGroup channels =
        new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg)
        throws Exception {
        if (msg.text().equals("ping")) {
            ctx.writeAndFlush(new TextWebSocketFrame("pong"));
            return;
        }
        
        // 解析JSON消息
        ChatMessage chatMessage = mapper.readValue(msg.text(), ChatMessage.class);
        
        // 广播给所有客户端
        String json = mapper.writeValueAsString(chatMessage);
        channels.writeAndFlush(new TextWebSocketFrame(json));
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        channels.add(ctx.channel());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        channels.remove(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}