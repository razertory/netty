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
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.EventLoop;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.ThrowableUtil;
import io.netty.util.internal.UnstableApi;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static io.netty.handler.codec.http2.Http2CodecUtil.isOutboundStream;
import static io.netty.handler.codec.http2.Http2CodecUtil.isStreamIdValid;
import static io.netty.util.internal.ObjectUtil.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * An HTTP/2 handler that creates child channels for each stream.
 *
 * <p>When a new stream is created, a new {@link Channel} is created for it. Applications send and
 * receive {@link Http2StreamFrame}s on the created channel. {@link ByteBuf}s cannot be processed by the channel;
 * all writes that reach the head of the pipeline must be an instance of {@link Http2StreamFrame}. Writes that reach
 * the head of the pipeline are processed directly by this handler and cannot be intercepted.
 *
 * <p>The child channel will be notified of user events that impact the stream, such as {@link
 * Http2GoAwayFrame} and {@link Http2ResetFrame}, as soon as they occur. Although {@code
 * Http2GoAwayFrame} and {@code Http2ResetFrame} signify that the remote is ignoring further
 * communication, closing of the channel is delayed until any inbound queue is drained with {@link
 * Channel#read()}, which follows the default behavior of channels in Netty. Applications are
 * free to close the channel in response to such events if they don't have use for any queued
 * messages.
 *
 * <p>Outbound streams are supported via the {@link Http2StreamBootstrap}.
 *
 * <p>{@link ChannelConfig#setMaxMessagesPerRead(int)} and {@link ChannelConfig#setAutoRead(boolean)} are supported.
 *
 * <h3>Reference Counting</h3>
 *
 * Some {@link Http2StreamFrame}s implement the {@link ReferenceCounted} interface, as they carry
 * reference counted objects (e.g. {@link ByteBuf}s). The multiplex codec will call {@link ReferenceCounted#retain()}
 * before propagating a reference counted object through the pipeline, and thus an application handler needs to release
 * such an object after having consumed it. For more information on reference counting take a look at
 * http://netty.io/wiki/reference-counted-objects.html
 *
 * <h3>Channel Events</h3>
 *
 * A child channel becomes active as soon as it is registered to an {@link EventLoop}. Therefore, an active channel
 * does not map to an active HTTP/2 stream immediately. Only once a {@link Http2HeadersFrame} has been successfully sent
 * or received, does the channel map to an active HTTP/2 stream. In case it is not possible to open a new HTTP/2 stream
 * (i.e. due to the maximum number of active streams being exceeded), the child channel receives an exception
 * indicating the cause and is closed immediately thereafter.
 *
 * <h3>Writability and Flow Control</h3>
 *
 * A child channel observes outbound/remote flow control via the channel's writability. A channel only becomes writable
 * when it maps to an active HTTP/2 stream and the stream's flow control window is greater than zero. A child channel
 * does not know about the connection-level flow control window. {@link ChannelHandler}s are free to ignore the
 * channel's writability, in which case the excessive writes will be buffered by the parent channel. It's important to
 * note that only {@link Http2DataFrame}s are subject to HTTP/2 flow control. So it's perfectly legal (and expected)
 * by a handler that aims to respect the channel's writability to e.g. write a {@link Http2DataFrame} even if the
 * channel is marked unwritable.
 */
@UnstableApi
public class Http2MultiplexCodec extends Http2FrameCodec {

    private static final ChannelFutureListener CHILD_CHANNEL_REGISTRATION_LISTENER = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            registerDone(future);
        }
    };

    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<DefaultHttp2StreamChannel> OUTBOUND_FLOW_CONTROL_WINDOW_UPDATER =
            AtomicLongFieldUpdater.newUpdater(DefaultHttp2StreamChannel.class, "outboundFlowControlWindow");

    /**
     * Used by subclasses to queue a close channel within the read queue. When read, it will close
     * the channel (using Unsafe) instead of notifying handlers of the message with {@code
     * channelRead()}. Additional inbound messages must not arrive after this one.
     */
    private static final Object CLOSE_MESSAGE = new Object();
    /**
     * Used to add a message to the {@link ChannelOutboundBuffer}, so as to have it re-evaluate its writability
     * state.
     */
    private static final Object REEVALUATE_WRITABILITY_MESSAGE = new Object();
    private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
    private static final ClosedChannelException CLOSED_CHANNEL_EXCEPTION = ThrowableUtil.unknownStackTrace(
            new ClosedChannelException(), DefaultHttp2StreamChannel.class, "doWrite(...)");
    /**
     * Number of bytes to consider non-payload messages, to determine when to stop reading. 9 is
     * arbitrary, but also the minimum size of an HTTP/2 frame. Primarily is non-zero.
     */
    private static final int ARBITRARY_MESSAGE_SIZE = 9;

    /**
     * Returns the flow-control size for DATA frames, and 0 for all other frames.
     */
    private static final class FlowControlledFrameSizeEstimator implements MessageSizeEstimator {

        static final FlowControlledFrameSizeEstimator INSTANCE = new FlowControlledFrameSizeEstimator();

        static final MessageSizeEstimator.Handle HANDLE_INSTANCE = new MessageSizeEstimator.Handle() {
            @Override
            public int size(Object msg) {
                return msg instanceof Http2DataFrame ? ((Http2DataFrame) msg).flowControlledBytes() : 0;
            }
        };

        @Override
        public Handle newHandle() {
            return HANDLE_INSTANCE;
        }
    }

    private final List<DefaultHttp2StreamChannel> channelsToFireChildReadComplete =
            new ArrayList<DefaultHttp2StreamChannel>();

    private final ChannelHandler inboundStreamHandler;

    private int initialOutboundStreamWindow = Http2CodecUtil.DEFAULT_WINDOW_SIZE;
    private boolean flushNeeded;

    /**
     * Construct a new handler whose child channels run in a different event loop.
     *
     * @param server {@code true} this is a server
     */
    public Http2MultiplexCodec(boolean server, ChannelHandler inboundStreamHandler) {
        super(server);
        this.inboundStreamHandler = checkSharable(checkNotNull(inboundStreamHandler, "inboundStreamHandler"));
    }

    Http2MultiplexCodec(Http2ConnectionEncoder encoder, Http2ConnectionDecoder decoder, Http2Settings initialSettings,
                    long gracefulShutdownTimeoutMillis,  ChannelHandler inboundStreamHandler) {
        super(encoder, decoder, initialSettings, gracefulShutdownTimeoutMillis);
        this.inboundStreamHandler = inboundStreamHandler;
    }

    private static ChannelHandler checkSharable(ChannelHandler handler) {
        if ((handler instanceof ChannelHandlerAdapter && !((ChannelHandlerAdapter) handler).isSharable()) ||
                !handler.getClass().isAnnotationPresent(Sharable.class)) {
            throw new IllegalArgumentException("The handler must be Sharable");
        }
        return handler;
    }

    private static void registerDone(ChannelFuture future) {
        // Handle any errors that occurred on the local thread while registering. Even though
        // failures can happen after this point, they will be handled by the channel by closing the
        // childChannel.
        DefaultHttp2StreamChannel childChannel = (DefaultHttp2StreamChannel) future.channel();
        if (future.cause() != null) {
            if (childChannel.isRegistered()) {
                childChannel.close();
            } else {
                childChannel.unsafe().closeForcibly();
            }
        } else {
            childChannel.setWritable();
        }
    }

    @Override
    public final void handlerAdded0(ChannelHandlerContext ctx) throws Exception {
        if (ctx.executor() != ctx.channel().eventLoop()) {
            throw new IllegalStateException("EventExecutor must be EventLoop of Channel");
        }
    }

    @Override
    public final void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved0(ctx);
        channelsToFireChildReadComplete.clear();
    }

    @Override
    final Http2MultiplexCodecStream newStream() {
        return new Http2MultiplexCodecStream(connection());
    }

    @Override
    final void onHttp2Frame(ChannelHandlerContext ctx, Http2Frame frame) {
        if (frame instanceof Http2StreamFrame) {
            Http2StreamFrame streamFrame = (Http2StreamFrame) frame;

            onHttp2StreamFrame(((Http2MultiplexCodecStream) streamFrame.stream()).channel, streamFrame);
        } else if (frame instanceof Http2GoAwayFrame) {
            onHttp2GoAwayFrame(ctx, (Http2GoAwayFrame) frame);
        } else if (frame instanceof Http2SettingsFrame) {
            Http2Settings settings = ((Http2SettingsFrame) frame).settings();
            if (settings.initialWindowSize() != null) {
                initialOutboundStreamWindow = settings.initialWindowSize();
            }
        }
    }

    @Override
    final void onHttp2StreamClosed(ChannelHandlerContext ctx, Http2FrameStream stream) {
        DefaultHttp2StreamChannel childChannel = ((Http2MultiplexCodecStream) stream).channel;
        if (childChannel != null) {
            childChannel.streamClosed();
        }
    }

    @Override
    final void onHttp2StreamActive(ChannelHandlerContext ctx, Http2FrameStream stream) {
        DefaultHttp2StreamChannel childChannel = newStreamChannel(stream);

        childChannel.pipeline().addLast(inboundStreamHandler);

        ChannelFuture future = ctx.channel().eventLoop().register(childChannel);
        if (future.isDone()) {
            registerDone(future);
        } else {
            future.addListener(CHILD_CHANNEL_REGISTRATION_LISTENER);
        }
    }

    // TODO: This is most likely not the best way to expose this, need to think more about it.
    final Http2StreamChannel newOutboundStream() {
        return newStreamChannel(newStream());
    }

    private DefaultHttp2StreamChannel newStreamChannel(Http2FrameStream stream) {
        DefaultHttp2StreamChannel childChannel = new DefaultHttp2StreamChannel(stream);
        return childChannel;
    }

    @Override
    final void onHttp2FrameStreamException(ChannelHandlerContext ctx, Http2FrameStreamException cause) {
        Http2FrameStream stream = cause.stream();
        DefaultHttp2StreamChannel childChannel = ((Http2MultiplexCodecStream) stream).channel;

        if (childChannel == null)  {
            // TODO: Should we log this ?
            return;
        }
        try {
            childChannel.pipeline().fireExceptionCaught(cause.getCause());
        } finally {
            childChannel.close();
        }
    }

    private void onHttp2StreamFrame(DefaultHttp2StreamChannel childChannel, Http2StreamFrame frame) {
        if (!childChannel.fireChildRead(frame)) {
            if (childChannel.fireChildReadComplete()) {
                flushNeeded = true;
            }

            // Just called fireChildReadComplete() no need to do again in channelReadComplete(...)
            childChannel.inStreamsToFireChildReadComplete = false;
        } else if (!childChannel.inStreamsToFireChildReadComplete) {
            channelsToFireChildReadComplete.add(childChannel);
            childChannel.inStreamsToFireChildReadComplete = true;
        }
    }

    private void onHttp2GoAwayFrame(ChannelHandlerContext ctx, final Http2GoAwayFrame goAwayFrame) {
        try {
            forEachActiveStream(new Http2FrameStreamVisitor() {
                @Override
                public boolean visit(Http2FrameStream stream) {
                    final int streamId = stream.id();
                    final DefaultHttp2StreamChannel childChannel = ((Http2MultiplexCodecStream) stream).channel;
                    if (streamId > goAwayFrame.lastStreamId() && isOutboundStream(connection().isServer(), streamId)) {
                        childChannel.pipeline().fireUserEventTriggered(goAwayFrame.retainedDuplicate());
                    }
                    return true;
                }
            });
        } catch (Http2Exception e) {
            // TODO: log me ?
            ctx.close();
        } finally {
            // We need to ensure we release the goAwayFrame.
            goAwayFrame.release();
        }
    }

    /**
     * Notifies any child streams of the read completion.
     */
    @Override
    public final void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        onChannelReadComplete(ctx);
        channelReadComplete0(ctx);
    }

    final void onChannelReadComplete(ChannelHandlerContext ctx)  {
        // If we have many child channel we can optimize for the case when multiple call flush() in
        // channelReadComplete(...) callbacks and only do it once as otherwise we will end-up with multiple
        // write calls on the socket which is expensive.
        int size = channelsToFireChildReadComplete.size();
        try {
            if (size != 0) {
                try {
                    for (int i = 0; i < size; ++i) {
                        DefaultHttp2StreamChannel childChannel = channelsToFireChildReadComplete.get(i);
                        if (childChannel.inStreamsToFireChildReadComplete) {
                            // Clear early in case fireChildReadComplete() causes it to need to be re-processed
                            childChannel.inStreamsToFireChildReadComplete = false;
                            if (childChannel.fireChildReadComplete()) {
                                flushNeeded = true;
                            }
                        }
                    }
                } finally {
                    channelsToFireChildReadComplete.clear();
                }
            }
        } finally {
            // We always flush as this is what Http2ConnectionHandler does for now.
            // TODO: I think this is not really necessary and we should be able to optimize this in the future.
            flush0(ctx);
        }
    }

    // Allow to override for testing
    void flush0(ChannelHandlerContext ctx) {
        flush(ctx);
    }

    // Allow to override for testing
    void onBytesConsumed(ChannelHandlerContext ctx, Http2FrameStream stream, int bytes) throws Http2Exception {
        consumeBytes(stream.id(), bytes);
    }

    // Allow to extend for testing
    static final class Http2MultiplexCodecStream extends DefaultHttp2FrameStream {
        public Http2MultiplexCodecStream(Http2Connection connection) {
            super(connection);
        }

        DefaultHttp2StreamChannel channel;
    }

    private final class DefaultHttp2StreamChannel extends AbstractChannel implements Http2StreamChannel {

        private final Http2StreamChannelConfig config = new Http2StreamChannelConfig(this);
        private final Http2FrameStream stream;

        // Needs to be volatile as it will be read from multiple threads.
        private volatile boolean closed;
        private boolean readInProgress;
        private Queue<Object> inboundBuffer;

        /** {@code true} after the first HEADERS frame has been written **/
        private boolean firstFrameWritten;

        /** {@code true} if a close without an error was initiated **/
        private boolean streamClosedWithoutError;

        /** {@code true} if stream is in {@link Http2MultiplexCodec#channelsToFireChildReadComplete}. **/
        boolean inStreamsToFireChildReadComplete;

        // Keeps track of flush calls in channelReadComplete(...) and aggregate these.
        private boolean inFireChannelReadComplete;
        private boolean flushPending;

        /**
         * The flow control window of the remote side i.e. the number of bytes this channel is allowed to send to the
         * remote peer. The window can become negative if a channel handler ignores the channel's writability. We are
         * using a long so that we realistically don't have to worry about underflow.
         */
        @SuppressWarnings("UnusedDeclaration")
        volatile long outboundFlowControlWindow;

        DefaultHttp2StreamChannel(Http2FrameStream stream) {
            super(ctx.channel());
            this.stream = stream;
            ((Http2MultiplexCodecStream) stream).channel = this;
        }

        @Override
        public Http2FrameStream stream() {
            return stream;
        }

        void streamClosed() {
            streamClosedWithoutError = true;
            fireChildRead(CLOSE_MESSAGE);
            ((Http2MultiplexCodecStream) stream).channel = null;
        }

        @Override
        public ChannelMetadata metadata() {
            return METADATA;
        }

        @Override
        public ChannelConfig config() {
            return config;
        }

        @Override
        public boolean isOpen() {
            return !closed;
        }

        @Override
        public boolean isActive() {
            return isOpen();
        }

        @Override
        public boolean isWritable() {
            if (isStreamIdValid(stream.id())
                    // So that the channel doesn't become active before the initial flow control window has been set.
                    && outboundFlowControlWindow > 0) {
                // Could be null if channel closed.
                ChannelOutboundBuffer buffer = unsafe().outboundBuffer();
                return buffer != null && buffer.isWritable();
            }
            return false;
        }

        @Override
        protected AbstractUnsafe newUnsafe() {
            return new Http2ChannelUnsafe();
        }

        @Override
        protected boolean isCompatible(EventLoop loop) {
            return loop == parent().eventLoop();
        }

        @Override
        protected SocketAddress localAddress0() {
            return parent().localAddress();
        }

        @Override
        protected SocketAddress remoteAddress0() {
            return parent().remoteAddress();
        }

        @Override
        protected void doBind(SocketAddress localAddress) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doDisconnect() throws Exception {
            doClose();
        }

        @Override
        protected void doClose() throws Exception {
            closed = true;

            // Only ever send a reset frame if the connection is still alove as otherwise it makes no sense at all
            // anyway.
            if (ctx.channel().isActive() && !streamClosedWithoutError && isStreamIdValid(stream().id())) {
                Http2StreamFrame resetFrame = new DefaultHttp2ResetFrame(Http2Error.CANCEL).stream(stream());
                write0(resetFrame);
                flush0();
            }

            if (inboundBuffer == null) {
                return;
            }
            for (;;) {
                Object msg = inboundBuffer.poll();
                if (msg == null) {
                    break;
                }
                ReferenceCountUtil.release(msg);
            }
        }

        @Override
        protected void doBeginRead() {
            if (readInProgress) {
                return;
            }

            final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
            allocHandle.reset(config());
            if (inboundBuffer == null || inboundBuffer.isEmpty()) {
                readInProgress = true;
                return;
            }

            boolean read = false;

            do {
                Object m = inboundBuffer.poll();
                if (m == null) {
                    break;
                }
                if (!doRead0(m, allocHandle)) {
                    allocHandle.readComplete();
                    if (read) {
                        pipeline().fireChannelReadComplete();
                    }

                    close();
                    return;
                }
                read = true;
            } while (allocHandle.continueReading());

            allocHandle.readComplete();
            pipeline().fireChannelReadComplete();
        }

        void setWritable() {
            assert !isWritable();
            incrementOutboundFlowControlWindow(initialOutboundStreamWindow);
            pipeline().fireChannelWritabilityChanged();
        }

        @Override
        protected void doWrite(ChannelOutboundBuffer in) throws Exception {
            if (closed) {
                throw CLOSED_CHANNEL_EXCEPTION;
            }

            try {
                for (;;) {
                    final Object msg = in.current();
                    if (msg == null) {
                        break;
                    }
                    if (msg == REEVALUATE_WRITABILITY_MESSAGE) {
                        in.remove();
                        continue;
                    }

                    final int bytes = FlowControlledFrameSizeEstimator.HANDLE_INSTANCE.size(msg);

                    /*
                     * The flow control window needs to be decrement before stealing the message from the buffer (and
                     * thereby decrementing the number of pending bytes). Else, when calling steal() the number of
                     * pending bytes could  be less than the writebuffer watermark (=flow control window) and thus
                     * trigger a  writability change.
                     *
                     * This code must never trigger a writability change. Only reading window updates or channel writes
                      * may change the channel's writability.
                     */
                    incrementOutboundFlowControlWindow(-bytes);

                    final ChannelPromise promise = in.steal();

                    if (promise.isCancelled()) {
                        // Write was cancelled so skip it.
                        continue;
                    }

                    // TODO(buchgr): Should we also the change the writability if END_STREAM is set?
                    try {
                        if (msg instanceof Http2StreamFrame) {
                            Http2StreamFrame frame = validateStreamFrame((Http2StreamFrame) msg);
                            frame.stream(stream());

                            if (!firstFrameWritten && !isStreamIdValid(stream().id())) {
                                if (!(frame instanceof Http2HeadersFrame)) {
                                    throw new IllegalArgumentException("The first frame must be a headers frame. Was: "
                                            + frame.name());
                                }
                                firstFrameWritten = true;
                                write0(frame).addListener(new ChannelFutureListener() {
                                    @Override
                                    public void operationComplete(ChannelFuture future) throws Exception {
                                        if (future.isSuccess()) {
                                            promise.setSuccess();
                                            setWritable();
                                        } else {
                                            promise.setFailure(future.cause());
                                            close();
                                        }
                                    }
                                });
                                return;
                            }
                        } else if (!(msg instanceof Http2GoAwayFrame)) {
                            String msgStr = msg.toString();
                            ReferenceCountUtil.release(msg);
                            throw new IllegalArgumentException(
                                    "Message must be an Http2GoAwayFrame or Http2StreamFrame: " + msgStr);
                        }
                        write0(msg).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (future.isSuccess()) {
                                    promise.setSuccess();
                                } else {
                                    promise.setFailure(future.cause());
                                    if (bytes > 0 && isActive()) {
                                        /*
                                         * Return the flow control window of the failed data frame.
                                         */
                                        incrementOutboundFlowControlWindow(bytes);
                                        reevaluateWritability();
                                    }
                                }
                            }
                        });
                    } catch (Throwable t) {
                        promise.tryFailure(t);
                    }
                }
            } finally {
                // ensure we always flush even if the write loop throws.
                flush0();
            }
        }

        private void flush0()  {
            // If we are current channelReadComplete(...) call we should just mark this Channel with a flush pending.
            // We will ensure we trigger ctx.flush() after we processed all Channels later on and so aggregate the
            // flushes. This is done as ctx.flush() is expensive when as it may trigger an write(...) or writev(...)
            // operation on the socket.
            if (inFireChannelReadComplete) {
                flushPending = true;
            } else {
                Http2MultiplexCodec.this.flush0(ctx);
            }
        }

        private ChannelFuture write0(Object msg) {
            ChannelPromise promise = ctx.newPromise();
            Http2MultiplexCodec.this.write(ctx, msg, promise);
            return promise;
        }

        /**
         * Receive a read message. This does not notify handlers unless a read is in progress on the
         * channel.
         */
        boolean fireChildRead(final Object msg) {
            assert eventLoop().inEventLoop();

            if (closed) {
                ReferenceCountUtil.release(msg);
            } else {
                if (readInProgress) {
                    assert inboundBuffer == null || inboundBuffer.isEmpty();
                    // Check for null because inboundBuffer doesn't support null; we want to be consistent
                    // for what values are supported.
                    RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
                    if (!doRead0(msg, allocHandle)) {
                        allocHandle.readComplete();
                        readInProgress = false;
                        close();
                    }
                    return allocHandle.continueReading();
                } else {
                    if (inboundBuffer == null) {
                        inboundBuffer = new ArrayDeque<Object>(4);
                    }
                    inboundBuffer.add(msg);
                }
            }

            return false;
        }

        boolean fireChildReadComplete() {
            assert eventLoop().inEventLoop();
            try {
                if (readInProgress) {
                    inFireChannelReadComplete = true;
                    readInProgress = false;
                    unsafe().recvBufAllocHandle().readComplete();
                    pipeline().fireChannelReadComplete();
                }
                return flushPending;
            } finally {
                inFireChannelReadComplete = false;
                flushPending = false;
            }
        }

        void incrementOutboundFlowControlWindow(int bytes) {
            if (bytes == 0) {
                return;
            }
            OUTBOUND_FLOW_CONTROL_WINDOW_UPDATER.addAndGet(this, bytes);
        }

        /**
         * Returns whether reads should continue. The only reason reads shouldn't continue is that the
         * channel was just closed.
         */
        private boolean doRead0(Object msg, RecvByteBufAllocator.Handle allocHandle) {
            int numBytesToBeConsumed = 0;
            if (msg instanceof Http2DataFrame) {
                numBytesToBeConsumed = ((Http2DataFrame) msg).flowControlledBytes();
                allocHandle.lastBytesRead(numBytesToBeConsumed);
            } else {
                if (msg == CLOSE_MESSAGE) {
                    return false;
                }
                if (msg instanceof Http2WindowUpdateFrame) {
                    Http2WindowUpdateFrame windowUpdate = (Http2WindowUpdateFrame) msg;
                    incrementOutboundFlowControlWindow(windowUpdate.windowSizeIncrement());
                    reevaluateWritability();
                    return true;
                }
                allocHandle.lastBytesRead(ARBITRARY_MESSAGE_SIZE);
            }
            allocHandle.incMessagesRead(1);
            pipeline().fireChannelRead(msg);

            if (numBytesToBeConsumed != 0) {
                try {
                    bytesConsumed(numBytesToBeConsumed);
                } catch (Http2Exception e) {
                    pipeline().fireExceptionCaught(e);
                }
            }
            return true;
        }

        private void reevaluateWritability() {
            ChannelOutboundBuffer buffer = unsafe().outboundBuffer();
            // If the buffer is not writable but should be writable, then write and flush a dummy object
            // to trigger a writability change.
            if (buffer != null && !buffer.isWritable()
                    && buffer.totalPendingWriteBytes() < config.getWriteBufferHighWaterMark()) {
                buffer.addMessage(REEVALUATE_WRITABILITY_MESSAGE, 1, voidPromise());
                unsafe().flush();
            }
        }

        private void bytesConsumed(final int bytes) throws Http2Exception {
            onBytesConsumed(ctx, stream, bytes);
        }

        private Http2StreamFrame validateStreamFrame(Http2StreamFrame frame) {
            if (frame.stream() != null && frame.stream() != stream) {
                String msgString = frame.toString();
                ReferenceCountUtil.release(frame);
                throw new IllegalArgumentException(
                        "Stream " + frame.stream() + " must not be set on the frame: " + msgString);
            }
            return frame;
        }

        private final class Http2ChannelUnsafe extends AbstractUnsafe {
            @Override
            public void connect(final SocketAddress remoteAddress,
                                SocketAddress localAddress, final ChannelPromise promise) {
                promise.setFailure(new UnsupportedOperationException());
            }
        }

        /**
         * {@link ChannelConfig} so that the high and low writebuffer watermarks can reflect the outbound flow control
         * window, without having to create a new {@link WriteBufferWaterMark} object whenever the flow control window
         * changes.
         */
        private final class Http2StreamChannelConfig extends DefaultChannelConfig {

            // TODO(buchgr): Overwrite the RecvByteBufAllocator. We only need it to implement max messages per read.
            Http2StreamChannelConfig(Channel channel) {
                super(channel);
            }

            /**
             * @deprecated  Use {@link #getWriteBufferWaterMark()} instead.
             */
            @Override
            @Deprecated
            public int getWriteBufferHighWaterMark() {
                int window = (int) min(Integer.MAX_VALUE, outboundFlowControlWindow);
                return max(0, window);
            }

            /**
             * @deprecated  Use {@link #getWriteBufferWaterMark()} instead.
             */
            @Override
            @Deprecated
            public int getWriteBufferLowWaterMark() {
                return getWriteBufferHighWaterMark();
            }

            @Override
            public MessageSizeEstimator getMessageSizeEstimator() {
                return FlowControlledFrameSizeEstimator.INSTANCE;
            }

            @Override
            public WriteBufferWaterMark getWriteBufferWaterMark() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator) {
                throw new UnsupportedOperationException();
            }

            /**
             * @deprecated  Use {@link #setWriteBufferWaterMark(WriteBufferWaterMark)} instead.
             */
            @Override
            @Deprecated
            public ChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
                throw new UnsupportedOperationException();
            }

            /**
             * @deprecated  Use {@link #setWriteBufferWaterMark(WriteBufferWaterMark)} instead.
             */
            @Override
            @Deprecated
            public ChannelConfig setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ChannelConfig setWriteBufferWaterMark(WriteBufferWaterMark writeBufferWaterMark) {
                throw new UnsupportedOperationException();
            }
        }
    }
}
