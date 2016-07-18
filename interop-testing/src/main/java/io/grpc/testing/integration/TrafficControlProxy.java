package io.grpc.testing.integration;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

import java.net.InetSocketAddress;

public class TrafficControlProxy {
  private int proxyPort = 5050;
  private String loopback = "127.127.127.127";
  private int serverPort;
  private Server server;

  void setUp(){

  }

}
