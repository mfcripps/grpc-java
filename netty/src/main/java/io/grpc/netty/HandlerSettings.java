package io.grpc.netty;

import com.google.common.annotations.VisibleForTesting;

/**
 * Allows autoFlowControl to be turned on and off from interop testing.
 */
@VisibleForTesting
public final class HandlerSettings {

  private static boolean autoFlowControlOn;

  static void setAutoWindow(AbstractNettyHandler handler) {
    handler.setAutoTuneFlowControl(autoFlowControlOn);
  }

  public static void autoWindowOn(boolean autoFlowControl) {
    autoFlowControlOn = autoFlowControl;
  }
}
