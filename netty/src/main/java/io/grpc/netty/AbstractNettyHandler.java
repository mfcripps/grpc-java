/*
 * Copyright 2015, Google Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *
 *    * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.grpc.netty;

import static io.netty.handler.codec.http2.Http2CodecUtil.getEmbeddedHttp2Exception;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;

/**
 * Base class for all Netty gRPC handlers. This class standardizes exception handling (always
 * shutdown the connection) as well as sending the initial connection window at startup.
 */
abstract class AbstractNettyHandler extends Http2ConnectionHandler {
  private static long GRACEFUL_SHUTDOWN_TIMEOUT = SECONDS.toMillis(5);
  private int initialConnectionWindow;
  private ChannelHandlerContext ctx;

  AbstractNettyHandler(Http2ConnectionDecoder decoder,
                       Http2ConnectionEncoder encoder,
                       Http2Settings initialSettings) {
    super(decoder, encoder, initialSettings);

    // Set the timeout for graceful shutdown.
    gracefulShutdownTimeoutMillis(GRACEFUL_SHUTDOWN_TIMEOUT);

    // Extract the connection window from the settings if it was set.
    this.initialConnectionWindow = initialSettings.initialWindowSize() == null ? -1 :
            initialSettings.initialWindowSize();
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    this.ctx = ctx;
    // Sends the connection preface if we haven't already.
    super.handlerAdded(ctx);
    sendInitialConnectionWindow();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    // Sends connection preface if we haven't already.
    super.channelActive(ctx);
    sendInitialConnectionWindow();
  }

  @Override
  public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    Http2Exception embedded = getEmbeddedHttp2Exception(cause);
    if (embedded == null) {
      // There was no embedded Http2Exception, assume it's a connection error. Subclasses are
      // responsible for storing the appropriate status and shutting down the connection.
      onError(ctx, cause);
    } else {
      super.exceptionCaught(ctx, cause);
    }
  }

  protected final ChannelHandlerContext ctx() {
    return ctx;
  }

  /**
   * Sends initial connection window to the remote endpoint if necessary.
   */
  private void sendInitialConnectionWindow() throws Http2Exception {
    if (ctx.channel().isActive() && initialConnectionWindow > 0) {
      Http2Stream connectionStream = connection().connectionStream();
      int currentSize = connection().local().flowController().windowSize(connectionStream);
      int delta = initialConnectionWindow - currentSize;
      decoder().flowController().incrementWindowSize(connectionStream, delta);
      initialConnectionWindow = -1;
      ctx.flush();
    }
  }

  protected class FlowControlPinger {

    private static final int BDP_MEASUREMENT_PING = 1234;
    private static final int MAX_WINDOW_SIZE = 8 * 1024 * 1024;

    private int pingCount;
    private int pingReturn;
    private boolean pinging;
    private int dataSizeSincePing;
    private ByteBuf payloadBuff;

    public boolean isPinging() {
      return pinging;
    }

    public void setPinging(boolean pingOut) {
      pinging = pingOut;
    }

    public void sendPing(ChannelHandlerContext ctx) {
      setDataSizeSincePing(0);
      if (payloadBuff == null) {
        initializePayloadBuf();
      }
      payloadBuff.retain();
      encoder().writePing(ctx, false, payloadBuff, ctx.newPromise());
      pingCount++;
    }

    public void updateWindow() throws Http2Exception {
      pingReturn++;
      // Calculate new window size by doubling the observed BDP, but cap at max window
      int targetWindow = Math.min(getDataSincePing() * 2, MAX_WINDOW_SIZE);
      setPinging(false);
      int currentWindow =
          decoder().flowController().initialWindowSize(connection().connectionStream());
      if (targetWindow > currentWindow) {
        int increase = targetWindow - currentWindow;
        decoder().flowController().incrementWindowSize(connection().connectionStream(), increase);
        decoder().flowController().initialWindowSize(targetWindow);
        Http2Settings settings = new Http2Settings();
        settings.initialWindowSize(targetWindow);
        frameWriter().writeSettings(ctx(), settings, ctx().newPromise());
      }
    }

    public void incrementDataSincePing(int increase) {
      int currentSize = getDataSincePing();
      setDataSizeSincePing(currentSize + increase);
    }

    public int payload() {
      return BDP_MEASUREMENT_PING;
    }

    public int maxWindow() {
      return MAX_WINDOW_SIZE;
    }
    @VisibleForTesting
    int getPingCount() {
      return pingCount;
    }

    @VisibleForTesting
    int getPingReturn() {
      return pingReturn;
    }

    @VisibleForTesting
    int getDataSincePing() {
      return dataSizeSincePing;
    }

    @VisibleForTesting
    void setDataSizeSincePing(int dataSize) {
      dataSizeSincePing = dataSize;
    }

    private void initializePayloadBuf() {
      payloadBuff = ctx().alloc().directBuffer(8);
      payloadBuff.writeLong(BDP_MEASUREMENT_PING);
    }
  }
}
