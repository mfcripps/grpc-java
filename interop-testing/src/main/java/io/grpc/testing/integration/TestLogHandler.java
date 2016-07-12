package io.grpc.testing.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

public class TestLogHandler extends Handler {

  private ArrayList<Integer> bdps = new ArrayList<Integer>();
  private ArrayList<Integer> windows = new ArrayList<Integer>();

  @Override
  public void publish(LogRecord record) {
    // TODO(mcripps): Auto-generated method stub
    String message = record.getMessage();
    if (message.contains("OBDP")) {
      System.out.println(message);
      String[] parts = message.split("[A-Za-z]+: ");
      bdps.add(Integer.parseInt(parts[1].trim()));
      windows.add(Integer.parseInt(parts[2].trim()));
    }

  }

  @Override
  public void flush() {
    // TODO(mcripps): Auto-generated method stub

  }

  @Override
  public void close() throws SecurityException {
    // TODO(mcripps): Auto-generated method stub

  }

  public int getLastWindow() {
    return windows.get(windows.size() - 1);
  }

  public int getLastOBDP() {
    Collections.sort(bdps);
    return bdps.get(bdps.size() - 1);
  }
}
