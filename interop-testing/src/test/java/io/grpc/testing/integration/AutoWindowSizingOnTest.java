package io.grpc.testing.integration;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import io.grpc.ManagedChannel;
import io.grpc.netty.HandlerSettings;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.testing.TestUtils;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;

import java.net.InetSocketAddress;

public class AutoWindowSizingOnTest extends AbstractInteropTest {

  private static String localhost = "127.0.0.1";
  private static int serverPort = TestUtils.pickUnusedPort();

  @BeforeClass
  public static void turnOnAutoWindow() {
    HandlerSettings.autoWindowOn(true);
    startStaticServer(NettyServerBuilder.forAddress(new InetSocketAddress(localhost, serverPort)));
  }

  @AfterClass
  public static void shutdown() {
    stopStaticServer();
  }

  @Override
  protected ManagedChannel createChannel() {
    return NettyChannelBuilder.forAddress(localhost, serverPort)
        .negotiationType(NegotiationType.PLAINTEXT)
        .build();
  }
}
