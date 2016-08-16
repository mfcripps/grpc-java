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
import java.util.concurrent.Callable;
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

  private static Logger logger = Logger.getLogger("io.grpc.netty.NettyClientHandler");
  private static Handler logHandler;
  private static ManagedChannel channel;
  private static Server server;
  private static String localhost = "127.0.0.1";
  private static TrafficControlProxy proxy;

  // TODO: make ports/proxy arguments
  private final int port = 5001;
  private final int proxyPort = 5050;
  private final boolean useProxy = true;

  private static final ThreadPoolExecutor executor =
      new ThreadPoolExecutor(1, 10, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
  private static final List<Integer> windows = new ArrayList<Integer>();

  @BeforeClass
  public static void setUp() {
    HandlerSettings.autoWindowOn(true);
    logger.setLevel(Level.FINEST);
    logHandler = new Handler() {

      @Override
      public void publish(LogRecord record) {
        String message = record.getMessage();
        System.out.println(message);
        if (message.contains("Window")) {
          windows.add(Integer.parseInt(message.split(": ")[1]));
        }
      }

      @Override
      public void flush() {}

      @Override
      public void close() throws SecurityException {}
    };
    logger.addHandler(logHandler);
  }

  @AfterClass
  public static void shutDownTests() {
    executor.shutdown();
  }

  @After
  public void endTest() throws IOException {
    proxy.shutDown();
    server.shutdown();
  }

  @Test
  public void lowBandLowLatency() throws InterruptedException, ExecutionException {
    resetProxy(LOW_BAND, LOW_LAT, TimeUnit.MILLISECONDS).get();
    resetConnection(REGULAR_WINDOW, REGULAR_WINDOW);
    doTest(LOW_BAND, LOW_LAT);
  }

  @Test
  public void lowBandHighLatency() throws InterruptedException, ExecutionException {
    resetProxy(LOW_BAND, HIGH_LAT, TimeUnit.MILLISECONDS).get();
    resetConnection(REGULAR_WINDOW, REGULAR_WINDOW);
    doTest(LOW_BAND, HIGH_LAT);
  }

  @Test
  public void highBandLowLatency() throws InterruptedException, ExecutionException {
    resetProxy(HIGH_BAND, LOW_LAT, TimeUnit.MILLISECONDS).get();
    resetConnection(REGULAR_WINDOW, REGULAR_WINDOW);
    doTest(HIGH_BAND, LOW_LAT);
  }

  @Test
  public void highBandHighLatency() throws InterruptedException, ExecutionException {
    resetProxy(HIGH_BAND, HIGH_LAT, TimeUnit.MILLISECONDS).get();
    resetConnection(BIG_WINDOW, BIG_WINDOW);
    doTest(HIGH_BAND, HIGH_LAT);
  }

  @Test
  public void verySmallWindow() throws InterruptedException, ExecutionException {
    resetProxy(MED_BAND, MED_LAT, TimeUnit.MILLISECONDS).get();
    resetConnection(REGULAR_WINDOW, TINY_WINDOW);
    doTest(MED_BAND, MED_LAT);
  }

  /**
   * Main testing method. Streams 4 MB of data from a server and records the final window and
   * average bandwidth usage
   *
   * @param bandwidth
   * @param latency
   */
  private void doTest(int bandwidth, int latency) throws InterruptedException {

    int streamSize = 4 * 1024 * 1024;

    TestServiceGrpc.TestService stub = TestServiceGrpc.newStub(channel);
    StreamingOutputCallRequest.Builder builder = StreamingOutputCallRequest.newBuilder();
    builder.addResponseParameters(ResponseParameters.newBuilder().setSize(streamSize));
    StreamingOutputCallRequest request = builder.build();

    TestStreamObserver observer = new TestStreamObserver();
    stub.streamingOutputCall(request, observer);
    observer.waitFor();

    // Don't actually need bw right now but keeping it around because it might be interesting
    long bw = (streamSize / (observer.getElapsedTime() / TimeUnit.SECONDS.toNanos(1)));
    long expectedWindow = (latency * (bandwidth / TimeUnit.SECONDS.toMillis(1))) * 2;
    int lastWindow = windows.get(windows.size() - 1);

    // deal with cases that either don't cause a window update or hit max window
    expectedWindow = Math.min(MAX_WINDOW, (Math.max(expectedWindow, REGULAR_WINDOW)));

    // Range looks large, but this allows for only one extra/missed window update
    // (one extra update causes a 2x difference and one missed update causes a .5x difference)
    assertEquals(expectedWindow, lastWindow);
    assertTrue((lastWindow < 2 * expectedWindow) && (expectedWindow < 2 * lastWindow));
  }

  /**
   * Resets client/server and their flow control windows
   *
   * @param serverFlowControlWindow
   * @param clientFlowControlWindow
   * @throws InterruptedException
   */
  private void resetConnection(int serverFlowControlWindow, int clientFlowControlWindow)
      throws InterruptedException {
    if (channel != null) {
      if (!channel.isShutdown()) {
        channel.shutdown();
        channel.awaitTermination(100, TimeUnit.MILLISECONDS);
      }
    }
    startServer(serverFlowControlWindow);
    int channelPort = useProxy ? proxyPort : port;
    channel = NettyChannelBuilder.forAddress(new InetSocketAddress(localhost, channelPort))
        .flowControlWindow(clientFlowControlWindow)
        .negotiationType(NegotiationType.PLAINTEXT)
        .build();
  }

  private void startServer(int serverFlowControlWindow) {
    ServerBuilder<?> builder = NettyServerBuilder.forAddress(new InetSocketAddress(localhost, port))
        .flowControlWindow(serverFlowControlWindow);
    List<ServerInterceptor> allInterceptors = ImmutableList.of();
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
   * Start a proxy with new bandwidth/latency settings.
   *
   * @param targetBPS
   * @param targetLatency
   * @param latencyUnits
   */
  private Future<?> resetProxy(final int targetBps, final int targetLatency,
      final TimeUnit latencyUnits) {
    return executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          proxy = new TrafficControlProxy(targetBps, targetLatency, latencyUnits);
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

    long startRequestNanos;
    long endRequestNanos;
    CountDownLatch latch = new CountDownLatch(1);

    public TestStreamObserver() {
      startRequestNanos = System.nanoTime();
    }

    @Override
    public void onNext(StreamingOutputCallResponse value) {}

    @Override
    public void onError(Throwable t) {
      latch.countDown();
      throw new RuntimeException(t);
    }

    @Override
    public void onCompleted() {
      long endRequestNanos = System.nanoTime();
      latch.countDown();
    }

    public Long getElapsedTime() {
      return endRequestNanos - startRequestNanos;
    }

    public void waitFor() throws InterruptedException {
      latch.await();
    }
  }
}
