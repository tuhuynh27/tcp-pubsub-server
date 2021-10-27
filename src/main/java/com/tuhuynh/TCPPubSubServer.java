package com.tuhuynh;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TCPPubSubServer {
    public static final ConcurrentMap<String, Deque<Channel>> topics = new ConcurrentHashMap<>();

    public static void main(String[] args) throws InterruptedException {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap
                .group(new NioEventLoopGroup(), new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2))
                .channel(NioServerSocketChannel.class)
                .childHandler(new HandlerInit());
        ChannelFuture sync = serverBootstrap.bind(1234).syncUninterruptibly();
        sync.channel().closeFuture().sync();
    }

    public static class HandlerInit extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) {
            ch.pipeline()
                    .addLast(new IdleStateHandler(0, 0, 60))
                    .addLast(new DelimiterBasedFrameDecoder(1024 * 1024 * 64, Delimiters.lineDelimiter()))
                    .addLast(new StringDecoder(CharsetUtil.UTF_8))
                    .addLast(new StringEncoder(CharsetUtil.UTF_8))
                    .addLast(new Handler());
        }
    }

    public static class Handler extends SimpleChannelInboundHandler<String> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
            String[] msgArr = msg.trim().split("\\s+");
            String key = msgArr[0];
            if (key.equalsIgnoreCase("subscribe")) {
                if (msgArr.length < 2) {
                    ctx.writeAndFlush("Invalid subscribe command\n");
                    return;
                }
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 1; i < msgArr.length; i++) {
                    String value = msgArr[i];
                    Deque<Channel> list = topics.get(value);
                    if (list == null) {
                        list = new ArrayDeque<>();
                    }
                    list.add(ctx.channel());
                    topics.put(value, list);
                    stringBuilder.append(value).append(i == msgArr.length - 1 ? "" : ", ");
                }
                ctx.writeAndFlush("Subscribed to " + stringBuilder + "\n");
            } else if (key.equalsIgnoreCase("unsubscribe")) {
                if (msgArr.length < 2) {
                    ctx.writeAndFlush("Invalid unsubscribe command\n");
                    return;
                }
                for (int i = 1; i < msgArr.length; i++) {
                    String value = msgArr[i];
                    Deque<Channel> list = topics.get(value);
                    if (list != null) {
                        list.remove(ctx.channel());
                        if (list.size() == 0) {
                            topics.remove(value);
                        }
                    }
                }
                ctx.writeAndFlush("Unsubscribed\n");
            } else if (key.equalsIgnoreCase("publish")) {
                if (msgArr.length != 3) {
                    ctx.writeAndFlush("Invalid publish command\n");
                    return;
                }
                String topic = msgArr[1];
                String value = msgArr[2];
                Deque<Channel> list = topics.get(topic);
                if (list != null) {
                    for (Channel channel : list) {
                        if (channel.isActive()) {
                            channel.writeAndFlush(value + "\n");
                        } else {
                            if (!channel.isOpen()) {
                                list.remove(channel);
                            }
                        }
                    }
                }
                ctx.writeAndFlush("Published to topic " + topic + "\n");
            } else {
                ctx.writeAndFlush("Unknown command: " + key.toLowerCase(Locale.ROOT) + "\n");
            }
        }
    }
}
