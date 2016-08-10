package io.grpc.testing.integration;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

public class TestLogHandler extends Handler {

  private ArrayList<Integer> windows = new ArrayList<Integer>();

  @Override
  public void publish(LogRecord record) {
    String message = record.getMessage();
    if (message.contains("Window")) {
      windows.add(Integer.parseInt(record.getParameters()[0].toString()));
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

}

