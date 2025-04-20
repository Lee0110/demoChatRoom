package com.lyl.demoChatRoom.netty;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;

/**
 * 自定义跨域处理
 */
public class CorsHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest) {
            FullHttpRequest request = (FullHttpRequest) msg;

            // 添加CORS头到请求对象
            if (request.headers().contains(HttpHeaderNames.ORIGIN)) {
                HttpResponse response = new DefaultHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

                response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
                response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, PUT, DELETE");
                response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type");

                // 如果是预检请求（OPTIONS），直接返回响应
                if (request.method() == HttpMethod.OPTIONS) {
                    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                    request.release();
                    return;
                }
            }
        }
        ctx.fireChannelRead(msg); // 继续传递请求
    }
}