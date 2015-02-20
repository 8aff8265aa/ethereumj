package testUDP;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.udt.UdtChannel;
import io.netty.channel.udt.nio.NioUdtProvider;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * UDT Message Flow Server
 * <p>
 * Echoes back any received data from a client.
 */
public final class UDPServer {

    static Channel channel;
    static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("log4j.properties");

        final ThreadFactory acceptFactory = new DefaultThreadFactory("accept");
        final ThreadFactory connectFactory = new DefaultThreadFactory("connect");
        final NioEventLoopGroup acceptGroup =
                new NioEventLoopGroup(1, acceptFactory, NioUdtProvider.MESSAGE_PROVIDER);
        final NioEventLoopGroup connectGroup =
                new NioEventLoopGroup(1, connectFactory, NioUdtProvider.MESSAGE_PROVIDER);

        // Configure the server.
        try {
            final ServerBootstrap boot = new ServerBootstrap();

            boot.group(acceptGroup, connectGroup)
                    .channelFactory(NioUdtProvider.MESSAGE_ACCEPTOR)
                    //.option(ChannelOption.SO_BACKLOG, 10)
                    //.handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(new ChannelInitializer<UdtChannel>() {
                        @Override
                        public void initChannel(final UdtChannel ch)
                                throws Exception {
                            ch.pipeline().addLast(
                                    new LoggingHandler(LogLevel.INFO),
                                    new P2PUDPHandler());
                        }
                    });
            // Start the server.
            channel = boot.bind(PORT).sync().channel();
            // Wait until the server socket is closed.
            channel.close().sync();
        } finally {
            // Shut down all event loops to terminate all threads.
            acceptGroup.shutdownGracefully();
            connectGroup.shutdownGracefully();

            acceptGroup.terminationFuture().syncUninterruptibly();
            connectGroup.terminationFuture().syncUninterruptibly();
        }
    }
}
