package io.grpc.testing.integration;

import com.google.common.collect.ImmutableList;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;

public class FlowControlServer {

  private static Server server;

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Usage: ./flowcontrol-server port");
      return;
    }
    int port = Integer.parseInt(args[0]);
    startServer(port);

  }

  static void startServer(int port) {
    ServerBuilder<?> builder = NettyServerBuilder.forPort(port).flowControlWindow(64 * 1024);
    List<ServerInterceptor> allInterceptors = ImmutableList.<ServerInterceptor>builder().build();
    builder.addService(ServerInterceptors.intercept(
        TestServiceGrpc.bindService(new TestServiceImpl(Executors.newScheduledThreadPool(2))),
        allInterceptors));
    try {
      server = builder.build().start();
      while (true) {
        // wait to die
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
