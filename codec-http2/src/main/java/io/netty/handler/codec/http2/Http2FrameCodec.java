/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.Http2Connection.PropertyKey;
import io.netty.handler.codec.http2.Http2Stream.State;
import io.netty.handler.codec.http2.StreamBufferingEncoder.Http2ChannelClosedException;
import io.netty.handler.codec.http2.StreamBufferingEncoder.Http2GoAwayException;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.codec.http.HttpServerUpgradeHandler.UpgradeEvent;
import io.netty.util.ReferenceCountUtil;

import io.netty.util.ReferenceCounted;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import static io.netty.handler.codec.http2.Http2CodecUtil.isOutboundStream;
import static io.netty.handler.codec.http2.Http2CodecUtil.isStreamIdValid;

/**
 * <p><em>This API is very immature.</em> The Http2Connection-based API is currently preferred over this API.
 * This API is targeted to eventually replace or reduce the need for the {@link Http2ConnectionHandler} API.
 *
 * <p>A HTTP/2 handler that maps HTTP/2 frames to {@link Http2Frame} objects and vice versa. For every incoming HTTP/2
 * frame, a {@link Http2Frame} object is created and propagated via {@link #channelRead}. Outbound {@link Http2Frame}
 * objects received via {@link #write} are converted to the HTTP/2 wire format. HTTP/2 frames specific to a stream
 * implement the {@link Http2StreamFrame} interface. The {@link Http2FrameCodec} is instantiated using the
 * {@link Http2FrameCodecBuilder}. It's recommended for channel handlers to inherit from the
 * {@link Http2ChannelDuplexHandler}, as it provides additional functionality like iterating over all active streams or
 * creating outbound streams.
 *
 * <h3>Stream Lifecycle</h3>
 *
 * The frame codec delivers and writes frames for active streams. An active stream is closed when either side sends a
 * {@code RST_STREAM} frame or both sides send a frame with the {@code END_STREAM} flag set. Each
 * {@link Http2StreamFrame} has a {@link Http2FrameStream} object attached that uniquely identifies a particular stream.
 *
 * <p>{@link Http2StreamFrame}s read from the channel always a {@link Http2FrameStream} object set, while when writing a
 * {@link Http2StreamFrame} the application code needs to set a {@link Http2FrameStream} object using
 * {@link Http2StreamFrame#stream(Http2FrameStream)}.
 *
 * <h3>Flow control</h3>
 *
 * The frame codec automatically increments stream and connection flow control windows. It's possible to customize
 * when flow control windows are updated via {@link Http2FrameCodecBuilder#windowUpdateRatio(float)}.
 *
 * <p>Incoming flow controlled frames need to be consumed by writing a {@link Http2WindowUpdateFrame} with the consumed
 * number of bytes and the corresponding stream identifier set to the frame codec.
 *
 * <p>The local stream-level flow control window can be changed by writing a {@link Http2SettingsFrame} with the
 * {@link Http2Settings#initialWindowSize()} set to the targeted value.
 *
 * <p>The connection-level flow control window can be changed by writing a {@link Http2WindowUpdateFrame} with the
 * desired window size <em>increment</em> in bytes and the stream identifier set to {@code 0}. By default the initial
 * connection-level flow control window is the same as initial stream-level flow control window.
 *
 * <h3>New inbound Streams</h3>
 *
 * The first frame of a HTTP/2 stream must be a {@link Http2HeadersFrame}, which will have a {@link Http2FrameStream}
 * object attached.
 *
 * <h3>New outbound Streams</h3>
 *
 * A outbound HTTP/2 stream can be created by first instantiating a new {@link Http2FrameStream} object via
 * {@link Http2ChannelDuplexHandler#newStream()}, and then writing a {@link Http2HeadersFrame} object with the stream
 * attached.
 *
 * <pre>
 *     final Http2Stream2 stream = handler.newStream();
 *     ctx.write(headersFrame.stream(stream)).addListener(new ChannelFutureListener() {
 *
 *         @Override
 *         public void operationComplete(ChannelFuture f) {
 *             if (f.isSuccess()) {
 *                 // Stream is active and stream.id() returns a valid stream identifier.
 *                 System.out.println("New stream with id " + stream.id() + " created.");
 *             } else {
 *                 // Stream failed to become active. Handle error.
 *                 if (f.cause() instanceof Http2NoMoreStreamIdsException) {
 *
 *                 } else if (f.cause() instanceof Http2GoAwayException) {
 *
 *                 } else {
 *
 *                 }
 *             }
 *         }
 *     }
 * </pre>
 *
 * <p>If a new stream cannot be created due to stream id exhaustion of the endpoint, the {@link ChannelPromise} of the
 * HEADERS frame will fail with a {@link Http2NoMoreStreamIdsException}.
 *
 * <p>The HTTP/2 standard allows for an endpoint to limit the maximum number of concurrently active streams via the
 * {@code SETTINGS_MAX_CONCURRENT_STREAMS} setting. When this limit is reached, no new streams can be created. However,
 * the {@link Http2FrameCodec} can be build with
 * {@link Http2FrameCodecBuilder#encoderEnforceMaxConcurrentStreams(boolean)} enabled, in which case a new stream and
 * its associated frames will be buffered until either the limit is increased or an active stream is closed. It's,
 * however, possible that a buffered stream will never become active. That is, the channel might
 * get closed or a GO_AWAY frame might be received. In the first case, all writes of buffered streams will fail with a
 * {@link Http2ChannelClosedException}. In the second case, all writes of buffered streams with an identifier less than
 * the last stream identifier of the GO_AWAY frame will fail with a {@link Http2GoAwayException}.
 *
 * <h3>Error Handling</h3>
 *
 * Exceptions and errors are propagated via {@link ChannelInboundHandler#exceptionCaught}. Exceptions that apply to
 * a specific HTTP/2 stream are wrapped in a {@link Http2FrameStreamException} and have the corresponding
 * {@link Http2FrameStream} object attached.
 *
 * <h3>Reference Counting</h3>
 *
 * Some {@link Http2StreamFrame}s implement the {@link ReferenceCounted} interface, as they carry
 * reference counted objects (e.g. {@link ByteBuf}s). The frame codec will call {@link ReferenceCounted#retain()} before
 * propagating a reference counted object through the pipeline, and thus an application handler needs to release such
 * an object after having consumed it. For more information on reference counting take a look at
 * http://netty.io/wiki/reference-counted-objects.html
 *
 * <h3>HTTP Upgrade</h3>
 *
 * Server-side HTTP to HTTP/2 upgrade is supported in conjunction with {@link Http2ServerUpgradeCodec}; the necessary
 * HTTP-to-HTTP/2 conversion is performed automatically.
 */
@UnstableApi
public class Http2FrameCodec extends Http2ConnectionHandler  {

    private static final InternalLogger LOG = InternalLoggerFactory.getInstance(Http2FrameCodec.class);

    private final PropertyKey streamKey;

    // Used to adjust flow control window on channel active. Set to null afterwards.
    private Integer initialLocalConnectionWindow;

    private ChannelHandlerContext ctx;

    /** Number of buffered streams if the {@link StreamBufferingEncoder} is used. **/
    private int numBufferedStreams;

    public Http2FrameCodec(boolean server) {
        this(server, null);
    }

    public Http2FrameCodec(boolean server, Http2FrameLogger frameLogger) {
        this(server, new DefaultHttp2FrameWriter(), frameLogger, Http2Settings.defaultSettings());
    }

    Http2FrameCodec(Http2ConnectionEncoder encoder, Http2ConnectionDecoder decoder, Http2Settings initialSettings,
                    long gracefulShutdownTimeoutMillis) {
        super(decoder, encoder, initialSettings);

        decoder.frameListener(new FrameListener());
        connection().addListener(new ConnectionListener());
        streamKey = connection().newKey();
        initialLocalConnectionWindow = initialSettings.initialWindowSize();
        gracefulShutdownTimeoutMillis(gracefulShutdownTimeoutMillis);
    }

    private Http2FrameCodec(boolean server, Http2FrameWriter frameWriter, Http2FrameLogger frameLogger,
                    Http2Settings initialSettings) {
        super(server, frameWriter, frameLogger, initialSettings);

        decoder().frameListener(new FrameListener());
        connection().addListener(new ConnectionListener());
        streamKey = connection().newKey();
        initialLocalConnectionWindow = initialSettings.initialWindowSize();
    }

    /**
     * Creates a new outbound/local stream.
     */
    DefaultHttp2FrameStream newStream() {
        assert ctx.executor().inEventLoop();

        return new DefaultHttp2FrameStream(connection());
    }

    /**
     * Iterates over all active HTTP/2 streams.
     *
     * <p>This method must not be called outside of the event loop.
     */
    final void forEachActiveStream(final Http2FrameStreamVisitor streamVisitor) throws Http2Exception {
        assert ctx.executor().inEventLoop();

        connection().forEachActiveStream(new Http2StreamVisitor() {
            @Override
            public boolean visit(Http2Stream stream) {
                Http2FrameStream stream2 = stream.getProperty(streamKey);
                if (stream2 == null) {
                    /**
                     * This code is expected to almost never execute. However, in rare cases it's possible that a
                     * stream is active without a {@link Http2FrameStream} object attached, as it's set in a listener of
                     * the HEADERS frame write.
                     */
                    // TODO(buchgr): Remove once Http2Stream2 and Http2Stream are merged.
                    return true;
                }
                try {
                    return streamVisitor.visit(stream2);
                } catch (Throwable cause) {
                    onError(ctx, cause);
                    return false;
                }
            }
        });
    }

    /**
     * Load any dependencies.
     */
    @Override
    public final void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.handlerAdded(ctx);
        handlerAdded0(ctx);
        sendInitialConnectionWindow(ctx);
    }

    void handlerAdded0(@SuppressWarnings("unsed") ChannelHandlerContext ctx) throws Exception { }

    @Override
    public final void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        sendInitialConnectionWindow(ctx);
    }

    private void sendInitialConnectionWindow(ChannelHandlerContext ctx) throws Http2Exception {
        if (ctx.channel().isActive() && initialLocalConnectionWindow != null) {
            Http2Stream connectionStream = connection().connectionStream();
            int currentSize = connection().local().flowController().windowSize(connectionStream);
            int delta = initialLocalConnectionWindow - currentSize;
            decoder().flowController().incrementWindowSize(connectionStream, delta);
            initialLocalConnectionWindow = null;
            ctx.flush();
        }
    }

    /**
     * Handles the cleartext HTTP upgrade event. If an upgrade occurred, sends a simple response via
     * HTTP/2 on stream 1 (the stream specifically reserved for cleartext HTTP upgrade).
     */
    @Override
    public final void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (!(evt instanceof UpgradeEvent)) {
            super.userEventTriggered(ctx, evt);
            return;
        }

        UpgradeEvent upgrade = (UpgradeEvent) evt;
        onUpgradeEvent(ctx, upgrade.retain());
        try {
            Http2Stream stream = connection().stream(Http2CodecUtil.HTTP_UPGRADE_STREAM_ID);
            // TODO: improve handler/stream lifecycle so that stream isn't active before handler added.
            // The stream was already made active, but ctx may have been null so it wasn't initialized.
            // https://github.com/netty/netty/issues/4942
            new ConnectionListener().onStreamActive(stream);
            upgrade.upgradeRequest().headers().setInt(
                    HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), Http2CodecUtil.HTTP_UPGRADE_STREAM_ID);
            new InboundHttpToHttp2Adapter(connection(), decoder().frameListener())
                    .channelRead(ctx, upgrade.upgradeRequest().retain());
        } finally {
            upgrade.release();
        }
    }

    /**
     * Processes all {@link Http2Frame}s. {@link Http2StreamFrame}s may only originate in child
     * streams.
     */
    @Override
    public final void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        write0(ctx, msg, promise);
    }

    // Allow to override for testing
    void write0(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof Http2DataFrame) {
            Http2DataFrame dataFrame = (Http2DataFrame) msg;
            encoder().writeData(ctx, dataFrame.stream().id(), dataFrame.content(),
                    dataFrame.padding(), dataFrame.isEndStream(), promise);
        } else if (msg instanceof Http2HeadersFrame) {
            writeHeadersFrame(ctx, (Http2HeadersFrame) msg, promise);
        } else if (msg instanceof Http2WindowUpdateFrame) {
            Http2WindowUpdateFrame frame = (Http2WindowUpdateFrame) msg;
            writeWindowUpdate(frame.stream().id(), frame.windowSizeIncrement(), promise);
        } else if (msg instanceof Http2ResetFrame) {
            Http2ResetFrame rstFrame = (Http2ResetFrame) msg;
            encoder().writeRstStream(ctx, rstFrame.stream().id(), rstFrame.errorCode(), promise);
        } else if (msg instanceof Http2PingFrame) {
            writePingFrame(ctx, (Http2PingFrame) msg, promise);
        } else if (msg instanceof Http2SettingsFrame) {
            writeSettingsFrame(ctx, (Http2SettingsFrame) msg, promise);
        } else if (msg instanceof Http2GoAwayFrame) {
            writeGoAwayFrame(ctx, (Http2GoAwayFrame) msg, promise);
        } else if (!(msg instanceof Http2Frame)) {
            ctx.write(msg, promise);
        } else {
            ReferenceCountUtil.release(msg);
            throw new UnsupportedMessageTypeException(msg);
        }
    }

    private void writePingFrame(ChannelHandlerContext ctx, Http2PingFrame pingFrame, ChannelPromise promise) {
       encoder().writePing(ctx, pingFrame.ack(), pingFrame.content(), promise);
    }

    private void writeWindowUpdate(int streamId, int bytes, ChannelPromise promise) {
        try {
            if (streamId == 0) {
                increaseInitialConnectionWindow(bytes);
            } else {
                consumeBytes(streamId, bytes);
            }
            promise.setSuccess();
        } catch (Throwable t) {
            promise.setFailure(t);
        }
    }

    private void increaseInitialConnectionWindow(int deltaBytes) throws Http2Exception {
        Http2LocalFlowController localFlow = connection().local().flowController();
        int targetConnectionWindow = localFlow.initialWindowSize() + deltaBytes;
        localFlow.incrementWindowSize(connection().connectionStream(), deltaBytes);
        localFlow.initialWindowSize(targetConnectionWindow);
    }

    final void consumeBytes(int streamId, int bytes) throws Http2Exception {
        Http2Stream stream = connection().stream(streamId);
        connection().local().flowController().consumeBytes(stream, bytes);
    }

    private void writeSettingsFrame(ChannelHandlerContext ctx, Http2SettingsFrame frame, ChannelPromise promise) {
        encoder().writeSettings(ctx, frame.settings(), promise);
    }

    private void writeGoAwayFrame(ChannelHandlerContext ctx, Http2GoAwayFrame frame, ChannelPromise promise) {
        if (frame.lastStreamId() > -1) {
            frame.release();
            throw new IllegalArgumentException("Last stream id must not be set on GOAWAY frame");
        }

        int lastStreamCreated = connection().remote().lastStreamCreated();
        int lastStreamId = lastStreamCreated + frame.extraStreamIds() * 2;
        // Check if the computation overflowed.
        if (lastStreamId < lastStreamCreated) {
            lastStreamId = Integer.MAX_VALUE;
        }
        goAway(ctx, lastStreamId, frame.errorCode(), frame.content(), promise);
    }

    private void writeHeadersFrame(
            final ChannelHandlerContext ctx, Http2HeadersFrame headersFrame, final ChannelPromise promise) {
        final int streamId;
        final ChannelPromise writePromise;
        if (isStreamIdValid(headersFrame.stream().id())) {
            streamId = headersFrame.stream().id();
            writePromise = promise;
        } else {
            final DefaultHttp2FrameStream stream = (DefaultHttp2FrameStream) headersFrame.stream();
            final Http2Connection connection = connection();
            streamId = connection.local().incrementAndGetNextStreamId();
            if (streamId < 0) {
                promise.setFailure(new Http2NoMoreStreamIdsException());
                return;
            }
            numBufferedStreams++;
            // Set the stream id before completing the promise, as any listener added by a user will be executed
            // before the below listener, and so the stream identifier is accessible in a user's listener.
            stream.id(streamId);
            // Ensure that the listener gets executed before any listeners a user might have attached.
            // TODO(buchgr): Once Http2Stream2 and Http2Stream are merged this is no longer necessary.
            writePromise = ctx.newPromise();
            writePromise.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    numBufferedStreams--;

                    Throwable cause = future.cause();
                    Http2Stream connectionStream;
                    if (cause == null && (connectionStream = connection.stream(streamId)) != null) {
                        connectionStream.setProperty(streamKey, stream);
                    } else {
                        onHttp2StreamClosed(ctx, stream);
                    }
                    if (cause == null) {
                        promise.setSuccess();
                    } else {
                        promise.setFailure(cause);
                    }
                }
            });
        }
        encoder().writeHeaders(ctx, streamId, headersFrame.headers(), headersFrame.padding(),
                                            headersFrame.isEndStream(), writePromise);
    }

    // TODO: Should we maybe also send Http2FrameStreamEvents for all the other stream state changes ?
    private final class ConnectionListener extends Http2ConnectionAdapter {

        @Override
        public void onStreamActive(Http2Stream stream) {
            if (isOutboundStream(connection().isServer(), stream.id())) {
                return;
            }

            DefaultHttp2FrameStream stream2 = newStream().id(stream.id());
            stream.setProperty(streamKey, stream2);
            onHttp2StreamActive(ctx, stream2);
        }

        @Override
        public void onStreamClosed(Http2Stream stream) {
            DefaultHttp2FrameStream stream2 = stream.getProperty(streamKey);
            if (stream2 != null) {
                onHttp2StreamClosed(ctx, stream2);
            }
        }

        @Override
        public void onGoAwayReceived(final int lastStreamId, long errorCode, ByteBuf debugData) {
            onHttp2Frame(ctx, new DefaultHttp2GoAwayFrame(lastStreamId, errorCode, debugData.retain()));
        }
    }

    @Override
    protected final void onConnectionError(ChannelHandlerContext ctx, Throwable cause, Http2Exception http2Ex) {
        ctx.fireExceptionCaught(cause);
    }

    /**
     * Exceptions for unknown streams, that is streams that have no {@link Http2FrameStream} object attached
     * are simply logged and replied to by sending a RST_STREAM frame. There is not much value in propagating such
     * exceptions through the pipeline, as a user will not have any additional information / state about this
     * stream and thus can't do any meaningful error handling.
     */
    @Override
    protected final void onStreamError(ChannelHandlerContext ctx, Throwable cause,
                                 Http2Exception.StreamException streamException) {
        int streamId = streamException.streamId();
        Http2Stream connectionStream = connection().stream(streamId);
        if (connectionStream == null) {
            LOG.warn("Stream exception thrown for unkown stream.", cause);
            // Write a RST_STREAM
            super.onStreamError(ctx, cause, streamException);
            return;
        }

        Http2FrameStream stream = connectionStream.getProperty(streamKey);
        if (stream == null) {
            LOG.warn("Stream exception thrown without stream object attached.", cause);
            // Write a RST_STREAM
            super.onStreamError(ctx, cause, streamException);
            return;
        }

        onHttp2FrameStreamException(ctx, new Http2FrameStreamException(stream, streamException.error(), cause));
    }

    @Override
    protected final boolean isGracefulShutdownComplete() {
        return super.isGracefulShutdownComplete() && numBufferedStreams == 0;
    }

    private final class FrameListener extends Http2FrameAdapter {
        @Override
        public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) {
            onHttp2Frame(ctx, new DefaultHttp2SettingsFrame(settings));
        }

        @Override
        public void onPingRead(ChannelHandlerContext ctx, ByteBuf data) {
            onHttp2Frame(ctx, new DefaultHttp2PingFrame(data.retain(), false));
        }

        @Override
        public void onPingAckRead(ChannelHandlerContext ctx, ByteBuf data) {
            onHttp2Frame(ctx, new DefaultHttp2PingFrame(data.retain(), true));
        }

        @Override
        public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
            onHttp2Frame(ctx, new DefaultHttp2ResetFrame(errorCode).stream(requireStream(streamId)));
        }

        @Override
        public void onWindowUpdateRead(ChannelHandlerContext ctx, int streamId, int windowSizeIncrement) {
            if (streamId == 0) {
                // Ignore connection window updates.
                return;
            }
            onHttp2Frame(ctx, new DefaultHttp2WindowUpdateFrame(windowSizeIncrement).stream(requireStream(streamId)));
        }

        @Override
        public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
                                  Http2Headers headers, int streamDependency, short weight, boolean
                                          exclusive, int padding, boolean endStream) {
            onHeadersRead(ctx, streamId, headers, padding, endStream);
        }

        @Override
        public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers,
                                  int padding, boolean endOfStream) {
            onHttp2Frame(ctx, new DefaultHttp2HeadersFrame(headers, endOfStream, padding)
                                        .stream(requireStream(streamId)));
        }

        @Override
        public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                              boolean endOfStream) {
            onHttp2Frame(ctx, new DefaultHttp2DataFrame(data.retain(), endOfStream, padding)
                                        .stream(requireStream(streamId)));
            // We return the bytes in consumeBytes() once the stream channel consumed the bytes.
            return 0;
        }

        private Http2FrameStream requireStream(int streamId) {
            Http2FrameStream stream = connection().stream(streamId).getProperty(streamKey);
            if (stream == null) {
                throw new IllegalStateException("Stream object required for identifier: " + streamId);
            }
            return stream;
        }
    }

    void onUpgradeEvent(ChannelHandlerContext ctx, UpgradeEvent evt) {
        ctx.fireUserEventTriggered(evt);
    }

    void onHttp2StreamActive(ChannelHandlerContext ctx, Http2FrameStream stream) {
        ctx.fireUserEventTriggered(Http2FrameStreamEvent.active(stream));
    }

    void onHttp2StreamClosed(ChannelHandlerContext ctx, Http2FrameStream stream) {
        ctx.fireUserEventTriggered(Http2FrameStreamEvent.closed(stream));
    }

    void onHttp2Frame(ChannelHandlerContext ctx, Http2Frame frame) {
        ctx.fireChannelRead(frame);
    }

    void onHttp2FrameStreamException(ChannelHandlerContext ctx, Http2FrameStreamException cause) {
        ctx.fireExceptionCaught(cause);
    }

    /**
     * {@link Http2FrameStream} implementation.
     */
    // TODO(buchgr): Merge Http2FrameStream and Http2Stream.
    static class DefaultHttp2FrameStream implements Http2FrameStream {

        private final Http2Connection connection;
        private volatile int id = -1;

        DefaultHttp2FrameStream(Http2Connection connection) {
            this.connection = connection;
        }

        DefaultHttp2FrameStream id(int id) {
            assert isStreamIdValid(id);
            this.id = id;
            return this;
        }

        @Override
        public int id() {
            return id;
        }

        @Override
        public State state() {
            Http2Stream stream0 = connection.stream(id);
            return stream0 == null
                    ? State.IDLE
                    : stream0.state();
        }

        @Override
        public String toString() {
            return String.valueOf(id);
        }
    }
}
