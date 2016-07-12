package io.grpc.testing.integration;

import com.google.protobuf.ByteString;

import org.junit.Test;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.StreamRecorder;
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
import io.netty.handler.codec.http2.DefaultHttp2LocalFlowController;
import io.netty.handler.codec.http2.Http2LocalFlowController;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class FlowControlTest extends AbstractInteropTest {

  private static Logger logger = null;
  private static TestLogHandler logHandler;

  private Clock clock;
  private ManagedChannel channel;
  private InetAddress addr;
  private int port = 5001;

  // in kbits
  private final int LOW_BAND = 800;
  private final int MED_BAND = 8000;
  private final int HIGH_BAND = 80000;

  // in milliseconds
  private final int LOW_LAT = 5;
  private final int MED_LAT = 125;
  private final int HIGH_LAT = 250;

  // in bytes
  private final int TINY_WINDOW = 1;
  private final int REGULAR_WINDOW = 64 * 1024;
  private final int BIG_WINDOW = 1024 * 1024;

  @Override
  public void setUp() {
    try {
      addr = InetAddress.getByName("127.127.127.127");
      NettyServerBuilder builder =
          NettyServerBuilder.forAddress(new InetSocketAddress("127.127.127.127", port))
              // .addService(ServerInterceptors.intercept(new flowControlServer()))
              .flowControlWindow(REGULAR_WINDOW);
      startStaticServer(builder);
      clock = Clock.systemUTC();
      logger = Logger.getLogger("io.grpc.netty.NettyClientHandler");
      logger.setLevel(Level.FINEST);
      logHandler = new TestLogHandler();
      logger.addHandler(logHandler);

    } catch (UnknownHostException e) {
      // do something
    }
  }

  public void doTests() {
    lowBandLowLatency();
    // lowBandHighLatency();
    // highBandLowLatency();
    // highBandHighLatency();
    // verySmallWindow();
    stopStaticServer();
  }

  private void lowBandLowLatency() {
    resetServer(REGULAR_WINDOW, REGULAR_WINDOW);
    doStream(LOW_BAND, LOW_LAT, 16 * 1024, 1, 5);
  }

  private void lowBandHighLatency() {
    resetServer(REGULAR_WINDOW, REGULAR_WINDOW);
    doStream(LOW_BAND, HIGH_LAT, 1 * 1024, 1, 5);
    // delay = 10000
  }

  private void highBandLowLatency() {
    resetServer(REGULAR_WINDOW, REGULAR_WINDOW);
    doStream(HIGH_BAND, LOW_LAT, 64 * 1024, 1, 5);
  }

  private void highBandHighLatency() {
    resetServer(REGULAR_WINDOW, REGULAR_WINDOW);
    doStream(HIGH_BAND, HIGH_LAT, 128 * 1024, 1, 5);
  }

  private void verySmallWindow() {
    resetServer(REGULAR_WINDOW, TINY_WINDOW);
    doStream(MED_BAND, MED_LAT, 16 * 1024, 1, 5);
  }

  private void somethingWithAlotOfStreams() {
    // TODO
  }

  private void somethingThatGetsToMaxWindow() {
    // TODO
  }

  private void somethingThatNeverCausesFlowControlUpdates() {
    // TODO
  }

  /**
   * Applies a qdisc with rate bandwidth and delay latency, and creates a stream request with a
   * response of size 'streamSize'.
   *
   * @param bandwidth (Kbit/s)
   * @param latency (miliseconds)
   * @param streamSize (bytes)
   */
  private void doStream(int bandwidth, int latency, int streamSize, int numStreams,
      int numRequests) {
    // setBandwidth(bandwidth, latency);

    ArrayList<Long> streamingTimes = new ArrayList<Long>();
    ArrayList<Long> completionTimes = new ArrayList<Long>();
    int payloadSize = streamSize;

    TestServiceGrpc.TestService stub = TestServiceGrpc.newStub(channel);
    StreamingOutputCallRequest.Builder builder = StreamingOutputCallRequest.newBuilder();
    for (int i = 0; i < 80000; i++) {
      builder.addResponseParameters(ResponseParameters.newBuilder().setSize(payloadSize));
    }
    StreamingOutputCallRequest request = builder.build();

    // warmup - do five requests to ramp up window
    ArrayList<TestStreamObserver> warmupObservers = new ArrayList<TestStreamObserver>();
    TestStreamObserver throwAway = new TestStreamObserver(stub, request, 5);
    warmupObservers.add(throwAway);
    stub.streamingOutputCall(request, throwAway);
    // for (TestStreamObserver throwAway : warmupObservers) {
      try {
        throwAway.waitFor();
      } catch (Exception e) {
        e.printStackTrace();
      }
    // }

    // test
    ArrayList<TestStreamObserver> observers = new ArrayList<TestStreamObserver>();
    for (int i = 0; i < 1; i++) {
      TestStreamObserver observer = new TestStreamObserver(stub, request, numRequests);
      observers.add(observer);
      stub.streamingOutputCall(request, observer);
    }

    for (TestStreamObserver observer : observers) {
      try {
        observer.waitFor();
        streamingTimes.addAll(observer.getElapsedTimes());
        completionTimes.addAll(observer.getCompletionTimes());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    double averageStreamTime = 0;
    for (int i = 0; i < streamingTimes.size(); i++) {
      averageStreamTime += streamingTimes.get(i) * .001;
    }


    averageStreamTime /= streamingTimes.size();
    double bwithUsed = (streamSize / averageStreamTime) * numStreams;
    System.out.println("OBDP: " + logHandler.getLastOBDP());
    System.out.println("Window: " + logHandler.getLastWindow());
    System.out.println("Bandwidth Used: " + bwithUsed + " Bytes/s");
    System.out.println("% Saturated: " + (bwithUsed / ((bandwidth / 8) * 1000)) * 100);
  }

  private void resetServer(int serverFlowControlWindow, int clientFlowControlWindow) {
    stopStaticServer();
    if (channel != null) {
      channel.shutdown();
    }
    startStaticServer(NettyServerBuilder.forAddress(new InetSocketAddress("127.127.127.127", port))
        .flowControlWindow(serverFlowControlWindow));
    // .addService(ServerInterceptors.intercept(new flowControlServer())));
    channel = NettyChannelBuilder.forAddress(new InetSocketAddress(addr, port))
        .flowControlWindow(clientFlowControlWindow)
        .negotiationType(NegotiationType.PLAINTEXT).build();
  }

  /**
   * Set netem rate in kbits and delay in ms NOTE: Had to set UID on tc (chmod 4755 /sbin/tc) to run
   * without sudo
   *
   * @param bandwidth
   */
  private void setBandwidth(int bandwidth, int delay) {
    Process tc;
    try {
      tc = Runtime.getRuntime().exec("tc qdisc del dev lo root");
      tc.waitFor();
      tc = Runtime.getRuntime().exec("tc qdisc add dev lo root handle 1: prio");
      tc.waitFor();
      tc = Runtime.getRuntime().exec("tc qdisc add dev lo parent 1:1 handle 2: netem rate "
          + bandwidth + "kbit delay " + delay + "ms");
      tc.waitFor();
      tc = Runtime.getRuntime().exec(
          "tc filter add dev lo parent 1:0 protocol ip prio 1 u32 match ip dst 127.127.127.127 flowid 2:1");
      tc.waitFor();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private class TestStreamObserver implements StreamObserver<StreamingOutputCallResponse> {
    int counter;
    ArrayList<Long> elapsedTimes = new ArrayList<Long>();
    ArrayList<Long> completionTimes = new ArrayList<Long>();
    long lastRequest;
    // TestServiceGrpc.TestService stub;
    // SimpleRequest request;
    CountDownLatch latch = new CountDownLatch(1);

    // StreamObserver<SimpleRequest> requestObserver;


    public TestStreamObserver(TestServiceGrpc.TestService stub, StreamingOutputCallRequest request,
        int numRequests) {
      // this.stub = stub;
      // this.request = request;
      counter = 2000;
      lastRequest = clock.millis();
    }

    @Override
    public void onNext(StreamingOutputCallResponse value) {

    }

    @Override
    public void onError(Throwable t) {
      t.printStackTrace();
      latch.countDown();
    }

    @Override
    public void onCompleted() {
      long now = clock.millis();
      completionTimes.add(now);
      elapsedTimes.add(now - lastRequest);
      // if (now - lastRequest < 10000) {
        // counter--;
        // lastRequest = clock.millis();
        // stub.streamingOutputCall(request, this);
      // } else {
        latch.countDown();
      // }
    }

    public ArrayList<Long> getElapsedTimes() {
      return new ArrayList<Long>(elapsedTimes);
    }

    public ArrayList<Long> getCompletionTimes() {
      return new ArrayList<Long>(completionTimes);
    }

    public void waitFor() throws Exception {
      latch.await();
    }
  }

  private class flowControlServer extends TestServiceGrpc.AbstractTestService {

    @Override
    public void unaryCall(SimpleRequest request,
        StreamObserver<SimpleResponse> responseObserver) {
      System.out.println("Hello");
    }
  }


  public static void main(String[] args) {
    FlowControlTest ft = new FlowControlTest();
    ft.setUp();
    ft.doTests();
  }

  @Override
  protected ManagedChannel createChannel() {
    // TODO(mcripps): Auto-generated method stub
    return null;
  }

}



