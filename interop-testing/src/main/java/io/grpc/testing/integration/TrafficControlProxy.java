package io.grpc.testing.integration;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class TrafficControlProxy {
  private int proxyPort = 5050;
  private String loopback = "127.127.127.127";
  private int serverPort = 5001;
  private Server server;
  private List<Message> serverQueue = new ArrayList<Message>();

  public void setUp() throws IOException {

    ServerSocket fromClient = new ServerSocket(proxyPort);
    Socket clientSock = fromClient.accept();
    System.out.println("Accepted a client");
    Socket serverSock = new Socket(loopback, serverPort);
      System.out.println("Connected to server");

    DataInputStream clientIn = new DataInputStream(clientSock.getInputStream());
    DataOutputStream clientOut = new DataOutputStream(serverSock.getOutputStream());
    DataInputStream serverIn = new DataInputStream(serverSock.getInputStream());
    DataOutputStream serverOut = new DataOutputStream(clientSock.getOutputStream());

    (new Thread(new ClientHandler(clientIn, clientOut))).start();
    (new Thread(new ServerHandler(serverIn, serverOut))).start();
  }


  public static void main(String[] args) {
    TrafficControlProxy p = new TrafficControlProxy();
    try {
      p.setUp();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private class ClientHandler implements Runnable {

    DataInputStream clientIn;
    DataOutputStream clientOut;

    public ClientHandler(DataInputStream clientIn, DataOutputStream clientOut) {
      this.clientIn = clientIn;
      this.clientOut = clientOut;
    }

    @Override
    public void run() {
      while (true) {
        try {
          byte[] request = new byte[1 * 1024];
          int readableRequestBytes = clientIn.read(request);
          clientOut.write(request, 0, readableRequestBytes);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

    }

  }

  private class ServerHandler implements Runnable {

    DataInputStream serverIn;
    DataOutputStream serverOut;

    public ServerHandler(DataInputStream serverIn, DataOutputStream serverOut) {
      this.serverIn = serverIn;
      this.serverOut = serverOut;
    }

    @Override
    public void run() {
      while (true) {
        try {
          byte[] response = new byte[1 * 1024];
          int readableResponseBytes = serverIn.read(response);
          serverOut.write(response, 0, readableResponseBytes);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private class Message {
    Long arrivalTime;
    byte[] message;
    int messageLength;

    public Message(Long at, byte[] m, int ml) {
      arrivalTime = at;
      message = m;
      messageLength = ml;
    }

  }

}
