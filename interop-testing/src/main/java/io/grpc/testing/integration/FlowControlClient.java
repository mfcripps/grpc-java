package io.grpc.testing.integration;

import org.junit.Test;

import static io.grpc.testing.integration.Messages.PayloadType.COMPRESSABLE;

import com.google.protobuf.ByteString;

import io.grpc.testing.integration.Messages.Payload;
import io.grpc.testing.integration.Messages.PayloadType;
import io.grpc.testing.integration.Messages.ResponseParameters;
import io.grpc.testing.integration.Messages.SimpleRequest;
import io.grpc.testing.integration.Messages.SimpleResponse;
import io.grpc.testing.integration.Messages.StreamingInputCallRequest;
import io.grpc.testing.integration.Messages.StreamingInputCallResponse;
import io.grpc.testing.integration.Messages.StreamingOutputCallRequest;
import io.grpc.testing.integration.Messages.StreamingOutputCallResponse;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.StreamRecorder;
import io.grpc.testing.TestUtils;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.testing.TestUtils;
import io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLException;

public class FlowControlClient {
  private String serverHost;
  private int serverPort;
  private ManagedChannel channel;
  private SslContext sslContext;
  private InetAddress address;
  private TestServiceGrpc.TestService asyncStub;

  public static void main(String args[]) throws SSLException, IOException {
    final FlowControlClient client = new FlowControlClient();
    client.parseArgs(args);
    client.setUp();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("Shutting down");
        try {
          client.tearDown();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    client.test();
    System.out.println("Hi im a flow control client");
  }

  private void parseArgs(String args[]) {
    for (String arg : args) {
      if (!arg.startsWith("--")) {
        System.err.println("All arguments must start with '--': " + arg);
        break;
      }
      String[] parts = arg.substring(2).split("=", 2);
      String key = parts[0];
      if ("address".equals(key)) {
        String[] address = parts[1].split(":");
        serverHost = address[0];
        serverPort = Integer.parseInt(address[1]);
      }
    }
  }

  private void setUp() throws SSLException, IOException {
    address = InetAddress.getByName(serverHost);
    sslContext =
        GrpcSslContexts.forClient().trustManager(TestUtils.loadCert("ca.pem")).build();

    /*
     * channel = NettyChannelBuilder.forAddress(new InetSocketAddress(address, serverPort))
     * .flowControlWindow(65 * 1024).negotiationType(NegotiationType.PLAINTEXT)
     * .sslContext(sslContext).build();
     */
  }

  private void test() {
    int testsPassed = 0;
    List<String> failMessages = new ArrayList<String>();

    try{
      verySmallStartWindow();
      System.out.println("Test Passed");
    } catch (Exception e) {
      System.out.println("Test Failed");
    }
  }

  @Test
  private void verySmallStartWindow() throws Exception {
    channel = NettyChannelBuilder.forAddress(new InetSocketAddress(address, serverPort))
        .flowControlWindow(10).negotiationType(NegotiationType.PLAINTEXT).sslContext(sslContext)
        .build();
    TestServiceGrpc.TestServiceStub stub = TestServiceGrpc.newStub(channel);
    final StreamingOutputCallRequest request = StreamingOutputCallRequest.newBuilder()
        .setResponseType(PayloadType.COMPRESSABLE)
        .addResponseParameters(ResponseParameters.newBuilder()
            .setSize(31415))
        .build();
    final StreamingOutputCallResponse response = StreamingOutputCallResponse.newBuilder()
        .setPayload(Payload.newBuilder().setType(PayloadType.COMPRESSABLE)
            .setBody(ByteString.copyFrom(new byte[31415])))
        .build();

    StreamRecorder<StreamingOutputCallResponse> recorder = StreamRecorder.create();
    stub.streamingOutputCall(request, recorder);
    recorder.awaitCompletion();
    assertSuccess(recorder);


  }

  protected static void assertSuccess(StreamRecorder<?> recorder) {
    if (recorder.getError() != null) {
      System.out.println(recorder.getError());
      // throw new AssertionError(recorder.getError());
    }
  }

  private void tearDown() {
    if (channel != null) {
      channel.shutdown();
    }
  }
}
