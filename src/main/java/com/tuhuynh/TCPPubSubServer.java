package com.tuhuynh;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class TCPPubSubServer {
    static {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        ctx.getLogger("io.netty").setLevel(Level.OFF);
    }

    private static int port = 1234;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(new Option("p", "port", true, "TCP Port"));
        CommandLine cmd = new DefaultParser().parse(options, args);
        Optional.ofNullable(cmd.getOptionValue("p")).ifPresent(str -> { port = Integer.parseInt(str); });
        new TCPPubSubServer().run();
    }

    public void run() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        serverBootstrap
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new HandlerInit());
        ChannelFuture sync = serverBootstrap.bind(port).syncUninterruptibly();
        try {
            System.out.println("\n" +
                    "      ___           \n" +
                    "|__/ |__  \\  /  /\\  \n" +
                    "|  \\ |___  \\/  /~~\\ \n" +
                    "                    \n");
            log.info("Server started at port " + port + ", PID: " + ProcessHandle.current().pid());
            sync.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            log.error("Error: ", e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            log.info("Bye!");
        }));
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
        public final ConcurrentMap<String, Set<Channel>> topics = new ConcurrentHashMap<>();

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
            String[] msgArr = msg.trim().split("\\s+");
            String key = msgArr[0];
            if (key.equalsIgnoreCase("subscribe")) {
                if (msgArr.length < 2) {
                    ctx.writeAndFlush("Invalid subscribe command\n");
                    return;
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i < msgArr.length; i++) {
                    String value = msgArr[i];
                    Set<Channel> list = topics.get(value);
                    if (list == null) {
                        list = new HashSet<>(10);
                    }
                    list.add(ctx.channel());
                    topics.put(value, list);
                    sb.append(value).append(", ");
                }
                if (sb.length() > 0){
                    sb.deleteCharAt(sb.length() - 2);
                }
                ctx.writeAndFlush("Subscribed to " + sb + "\n");
            } else if (key.equalsIgnoreCase("unsubscribe")) {
                if (msgArr.length < 2) {
                    ctx.writeAndFlush("Invalid unsubscribe command\n");
                    return;
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i < msgArr.length; i++) {
                    String value = msgArr[i];
                    Set<Channel> list = topics.get(value);
                    if (list != null) {
                        list.remove(ctx.channel());
                        if (list.size() == 0) {
                            topics.remove(value);
                        }
                    }
                    sb.append(value).append(", ");
                }
                if (sb.length() > 0){
                    sb.deleteCharAt(sb.length() - 2);
                }
                ctx.writeAndFlush("Unsubscribed to " + sb + "\n");
            } else if (key.equalsIgnoreCase("publish")) {
                if (msgArr.length != 3) {
                    ctx.writeAndFlush("Invalid publish command\n");
                    return;
                }
                String topic = msgArr[1];
                String value = msgArr[2];
                Set<Channel> set = topics.get(topic);
                if (set != null) {
                    for (Channel channel : set) {
                        if (channel.isActive()) {
                            channel.writeAndFlush("Message received from " + topic + ": " + value + "\n");
                        } else {
                            if (!channel.isOpen()) {
                                set.remove(channel);
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
