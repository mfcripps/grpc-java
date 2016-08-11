package io.grpc.testing.integration;

import io.grpc.ManagedChannel;
import io.grpc.netty.HandlerSettings;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.testing.integration.Messages.Payload;
import io.grpc.testing.integration.Messages.PayloadType;
import io.grpc.testing.integration.Messages.ResponseParameters;
import io.grpc.testing.integration.Messages.SimpleRequest;
import io.grpc.testing.integration.Messages.SimpleResponse;
import io.grpc.testing.integration.Messages.StreamingInputCallRequest;
import io.grpc.testing.integration.Messages.StreamingInputCallResponse;
import io.grpc.testing.integration.Messages.StreamingOutputCallRequest;
import io.grpc.testing.integration.Messages.StreamingOutputCallResponse;

public class FlowControlClient {

  private static Logger logger;
  private static TestLogHandler logHandler;
  private static ManagedChannel channel;

  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Usage: ./flowcontrol-client ip port flowcontrol");
    }
    String ip = args[0];
    int port = Integer.parseInt(args[1]);
    if (args.length == 3) {
      HandlerSettings.autoWindowOn(true);
    } else {
      HandlerSettings.autoWindowOn(false);
    }
    FlowControlClient client = new FlowControlClient();
    client.setUp();
    client.createChannel(ip, port);
    client.doStream();
  }

  void setUp() {
    logger = Logger.getLogger("io.grpc.netty.NettyClientHandler");
    logger.setLevel(Level.FINEST);
    logHandler = new TestLogHandler();
    logger.addHandler(logHandler);
  }

  void doStream() {
    int streamSize = 1024 * 1024;

    TestServiceGrpc.TestService stub = TestServiceGrpc.newStub(channel);
    StreamingOutputCallRequest.Builder builder = StreamingOutputCallRequest.newBuilder();
    builder.addResponseParameters(ResponseParameters.newBuilder().setSize(streamSize));
    StreamingOutputCallRequest request = builder.build();

    List<Long> times = new ArrayList<Long>();
    for (int i = 0; i < 100; i++) {
      TestStreamObserver observer = new TestStreamObserver();
      stub.streamingOutputCall(request, observer);
      try {
        observer.waitFor();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      times.add(observer.getElapsedTime());
    }
    int lastWindow = 64 * 1024;
    try {
      lastWindow = logHandler.getLastWindow();
    } catch (Exception e) {
      // System.out.println("log handler error");
    }

    for (int i = 0; i < times.size(); i++) {
      System.out.println(times.get(i));
    }
    System.out.println("Window: " + lastWindow);
  }

  void createChannel(String ip, int port) {
    channel = NettyChannelBuilder.forAddress(new InetSocketAddress(ip, port))
        .flowControlWindow(64 * 1024).negotiationType(NegotiationType.PLAINTEXT).build();
  }

  private class TestStreamObserver implements StreamObserver<StreamingOutputCallResponse> {

    long lastRequest;
    long elapsedTime;
    CountDownLatch latch = new CountDownLatch(1);

    public TestStreamObserver() {
      lastRequest = System.nanoTime();
    }

    @Override
    public void onNext(StreamingOutputCallResponse value) {}

    @Override
    public void onError(Throwable t) {
      t.printStackTrace();
      latch.countDown();
    }

    @Override
    public void onCompleted() {
      long now = System.nanoTime();
      elapsedTime = now - lastRequest;
      latch.countDown();
    }

    public Long getElapsedTime() {
      return elapsedTime;
    }

    public void waitFor() throws Exception {
      latch.await();
    }
  }

}
