package io.grpc.testing.integration;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.HandlerSettings;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.StreamRecorder;
import io.grpc.testing.TestUtils;
import io.grpc.testing.integration.Messages.Payload;
import io.grpc.testing.integration.Messages.PayloadType;
import io.grpc.testing.integration.Messages.ResponseParameters;
import io.grpc.testing.integration.Messages.SimpleRequest;
import io.grpc.testing.integration.Messages.SimpleResponse;
import io.grpc.testing.integration.Messages.StreamingInputCallRequest;
import io.grpc.testing.integration.Messages.StreamingInputCallResponse;
import io.grpc.testing.integration.Messages.StreamingOutputCallRequest;
import io.grpc.testing.integration.Messages.StreamingOutputCallResponse;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http2.DefaultHttp2LocalFlowController;
import io.netty.handler.codec.http2.Http2LocalFlowController;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.text.MessageFormat;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

@RunWith(JUnit4.class)
public class NettyFlowControlTest {

  // in bytes
  private static final int LOW_BAND = 1024 * 1024;
  private static final int MED_BAND = 2 * 1024 * 1024;
  private static final int HIGH_BAND = 5 * 1024 * 1024;

  // in milliseconds
  private static final int LOW_LAT = 50;
  private static final int MED_LAT = 100;
  private static final int HIGH_LAT = 200;

  // in bytes
  private static final int TINY_WINDOW = 1;
  private static final int REGULAR_WINDOW = 64 * 1024;
  private static final int BIG_WINDOW = 1024 * 1024;
  private static final int MAX_WINDOW = 8 * 1024 * 1024;

  private static Logger logger;
  private static TestLogHandler logHandler;
  private static ManagedChannel channel;
  private static InetAddress addr;
  private static Server server;
  private static String loopback = "127.0.0.1";

  // TODO: make ports/proxy arguments
  private final int port = 5001;
  private final int proxyPort = 5050;
  private final boolean useProxy = true;
  private int testsPassed;
  private int testsFailed;

  private static final ThreadPoolExecutor executor =
      new ThreadPoolExecutor(1, 10, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
  private static final TrafficControlProxy proxy =
      new TrafficControlProxy(LOW_BAND, LOW_LAT, TimeUnit.MILLISECONDS);

  // HandlerAccessor accessor = new HandlerAccessor();

  /*
   * Keeping this main class around for now because it's convenient to be able to run this from the
   * command line.
   */
  public static void main(String[] args) throws UnknownHostException {
    NettyFlowControlTest ft = new NettyFlowControlTest();
    setUp();
    ft.lowBandLowLatency();
    ft.lowBandHighLatency();
    ft.highBandLowLatency();
    ft.highBandHighLatency();
    ft.verySmallWindow();
    shutDownTests();
    ft.printResults();
  }

  public void printResults() {
    System.out.println("Passed: " + testsPassed + " Failed: " + testsFailed);
  }

  @BeforeClass
  public static void setUp() throws UnknownHostException {
    HandlerSettings.autoWindowOn(true);
    addr = InetAddress.getByName(loopback);
    logger = Logger.getLogger("io.grpc.netty.NettyClientHandler");
    logger.setLevel(Level.FINEST);
    logHandler = new TestLogHandler();
    logger.addHandler(logHandler);
    startProxy();
  }

  @AfterClass
  public static void shutDownTests() {
    try {
      proxy.shutDown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    executor.shutdown();
    server.shutdown();
  }


  @Test
  public void doTests() {
    lowBandLowLatency();
    lowBandHighLatency();
    highBandLowLatency();
    highBandHighLatency();
    verySmallWindow();
    assertEquals(0, testsFailed);
  }

  public void endTest() {
    server.shutdown();
  }

  // It is necessary to restart the server every time in order to bring the window back down.
  private void lowBandLowLatency() {
    resetConnection(REGULAR_WINDOW, REGULAR_WINDOW);
    doTest(LOW_BAND, LOW_LAT);
    endTest();
  }

  private void lowBandHighLatency() {
    resetProxy(LOW_BAND, HIGH_LAT, TimeUnit.MILLISECONDS);
    resetConnection(REGULAR_WINDOW, REGULAR_WINDOW);
    doTest(LOW_BAND, HIGH_LAT);
    endTest();
  }

  private void highBandLowLatency() {
    resetProxy(HIGH_BAND, LOW_LAT, TimeUnit.MILLISECONDS);
    resetConnection(REGULAR_WINDOW, REGULAR_WINDOW);
    doTest(HIGH_BAND, LOW_LAT);
    endTest();
  }

  private void highBandHighLatency() {
    resetProxy(HIGH_BAND, HIGH_LAT, TimeUnit.MILLISECONDS);
    resetConnection(REGULAR_WINDOW, REGULAR_WINDOW);
    doTest(HIGH_BAND, HIGH_LAT);
    endTest();
  }

  private void verySmallWindow() {
    resetProxy(MED_BAND, MED_LAT, TimeUnit.MILLISECONDS);
    resetConnection(REGULAR_WINDOW, TINY_WINDOW);
    doTest(MED_BAND, MED_LAT);
    endTest();
  }

  /**
   * Main testing method. Streams 4 MB of data from a server and records the final window and
   * average bandwidth usage
   *
   * @param bandwidth
   * @param latency
   */
  private void doTest(int bandwidth, int latency) {
    // arbitrary. smallest stream size I tried that still left enough time to fully inflate the
    // window
    int streamSize = 4 * 1024 * 1024;

    TestServiceGrpc.TestService stub = TestServiceGrpc.newStub(channel);
    StreamingOutputCallRequest.Builder builder = StreamingOutputCallRequest.newBuilder();
    builder.addResponseParameters(ResponseParameters.newBuilder().setSize(streamSize));
    StreamingOutputCallRequest request = builder.build();

    TestStreamObserver observer = new TestStreamObserver();
    stub.streamingOutputCall(request, observer);
    try {
      observer.waitFor();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Don't actually need bwithUsed right now but keeping it around because it might be interesting
    long bwithUsed = (streamSize / (observer.getElapsedTime() / TimeUnit.SECONDS.toNanos(1)));
    long expectedWindow = (latency * (bandwidth / TimeUnit.SECONDS.toMillis(1))) * 2;
    int lastWindow = logHandler.getLastWindow();

    // deal with cases that either don't cause a window update or hit max window
    expectedWindow = Math.min(MAX_WINDOW, (Math.max(expectedWindow, REGULAR_WINDOW)));

    // I know this range looks large, but this allows for only one extra/missed window update
    // (one extra update causes a 2x difference and one missed update causes a .5x difference)
    if ((lastWindow < 2.1 * expectedWindow) && (expectedWindow < 2.1 * lastWindow)) {
      System.out.println("PASS");
      testsPassed++;
    } else {
      System.out.println("FAIL");
      testsFailed++;
    }
  }

  /**
   * Resets client/server and their flow control windows
   *
   * @param serverFlowControlWindow
   * @param clientFlowControlWindow
   */
  private void resetConnection(int serverFlowControlWindow, int clientFlowControlWindow) {
    if (channel != null) {
      if (!channel.isShutdown()) {
        channel.shutdown();
      }
    }
    startServer(serverFlowControlWindow);

    int channelPort = port;
    if (useProxy) {
      channelPort = proxyPort;
    }
    channel = NettyChannelBuilder.forAddress(new InetSocketAddress(loopback, channelPort))
        .flowControlWindow(clientFlowControlWindow).negotiationType(NegotiationType.PLAINTEXT)
        .build();
  }

  private void startServer(int serverFlowControlWindow) {
    ServerBuilder<?> builder = NettyServerBuilder.forAddress(new InetSocketAddress(loopback, port))
        .flowControlWindow(serverFlowControlWindow);
    List<ServerInterceptor> allInterceptors = ImmutableList.<ServerInterceptor>builder()
        .build();
    builder.addService(ServerInterceptors.intercept(
        TestServiceGrpc.bindService(new TestServiceImpl(Executors.newScheduledThreadPool(2))),
        allInterceptors));
    try {
      server = builder.build().start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Restart proxy with new bandwidth/latency settings.
   *
   * @param targetBPS
   * @param targetLatency
   * @param latencyUnits
   */
  private void resetProxy(final int targetBPS, final int targetLatency,
      final TimeUnit latencyUnits) {
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          proxy.reset(targetBPS, targetLatency, latencyUnits);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
    try {
      // give the a few millis to start before trying to re-connect. Without this, the first request
      // sometimes throws an HTTP/2 error for a bad message.
      Thread.sleep(10);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void startProxy() {
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          proxy.start();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }


  /**
   * Simple stream observer to measure elapsed time of the call.
   */
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