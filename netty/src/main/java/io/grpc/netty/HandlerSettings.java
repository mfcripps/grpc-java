package io.grpc.netty;

import java.util.ArrayList;

public class HandlerSettings {

  static boolean autoFlowControlOn;

  static void setAutoWindow(AbstractNettyHandler handler) {
    handler.setAutoTuneFlowControl(autoFlowControlOn);
  }

  public static void autoWindowOn(boolean autoFlowControl) {
    autoFlowControlOn = autoFlowControl;
  }
}
