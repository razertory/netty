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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.DefaultChannelPipeline;
import io.netty.channel.DefaultMaxMessagesRecvByteBufAllocator;
import io.netty.channel.EventLoop;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.VoidChannelPromise;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.util.DefaultAttributeMap;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.ThrowableUtil;
import io.netty.util.internal.UnstableApi;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
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

    private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
    private static final ClosedChannelException CLOSED_CHANNEL_EXCEPTION = ThrowableUtil.unknownStackTrace(
            new ClosedChannelException(), DefaultHttp2StreamChannel.Http2ChannelUnsafe.class, "write(...)");
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

    private static final class Http2StreamChannelRecvByteBufAllocator extends DefaultMaxMessagesRecvByteBufAllocator {

        static final Http2StreamChannelRecvByteBufAllocator INSTANCE = new Http2StreamChannelRecvByteBufAllocator();

        @Override
        public MaxMessageHandle newHandle() {
            return new MaxMessageHandle() {
                @Override
                public int guess() {
                    return 1024;
                }
            };
        }
    }

    private final ChannelHandler inboundStreamHandler;

    private int initialOutboundStreamWindow = Http2CodecUtil.DEFAULT_WINDOW_SIZE;
    private boolean flushNeeded;
    private long idCount;

    // Linked-List for DefaultHttp2StreamChannel instances that need to be processed by channelReadComplete(...)
    private DefaultHttp2StreamChannel head;
    private DefaultHttp2StreamChannel tail;

    // volatile as it is accessed from the DefaultHttp2StreamChannel in a multi-threaded way.
    volatile ChannelHandlerContext ctx;

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

    static ChannelHandler checkSharable(ChannelHandler handler) {
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
        if (!future.isSuccess()) {
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
        this.ctx = ctx;
    }

    @Override
    public final void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved0(ctx);

        DefaultHttp2StreamChannel ch = head;
        while (ch != null) {
            DefaultHttp2StreamChannel curr = ch;
            ch = curr.next;
            curr.next = null;
        }
        head = tail = null;
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
        DefaultHttp2StreamChannel childChannel = new DefaultHttp2StreamChannel(stream);
        ChannelFuture future = ctx.channel().eventLoop().register(childChannel);
        if (future.isDone()) {
            registerDone(future);
        } else {
            future.addListener(CHILD_CHANNEL_REGISTRATION_LISTENER);
        }
    }

    // TODO: This is most likely not the best way to expose this, need to think more about it.
    final Http2StreamChannel newOutboundStream() {
        return new DefaultHttp2StreamChannel(newStream());
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
            childChannel.unsafe().closeForcibly();
        }
    }

    private void onHttp2StreamFrame(DefaultHttp2StreamChannel childChannel, Http2StreamFrame frame) {
        if (!childChannel.fireChildRead(frame)) {
            if (childChannel.fireChildReadComplete()) {
                flushNeeded = true;
            }

            // Just called fireChildReadComplete() no need to do again in channelReadComplete(...)
            childChannel.fireChannelReadPending = false;
        } else if (!childChannel.fireChannelReadPending) {

            assert childChannel.next == null;

            if (tail == null) {
                assert head == null;
                tail = head = childChannel;
            } else {
                tail.next = childChannel;
                tail = childChannel;
            }
            childChannel.fireChannelReadPending = true;
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

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        // TODO: Should we also notify all the Http2StreamChannel about this and so trigger a writability change ?
        super.channelWritabilityChanged(ctx);
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
        try {
            DefaultHttp2StreamChannel current = head;
            while (current != null) {
                DefaultHttp2StreamChannel childChannel = current;
                if (childChannel.fireChannelReadPending) {
                    // Clear early in case fireChildReadComplete() causes it to need to be re-processed
                    childChannel.fireChannelReadPending = false;
                    if (childChannel.fireChildReadComplete()) {
                        flushNeeded = true;
                    }
                }
                childChannel.next = null;
                current = current.next;
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

    // TODO: Handle writability changes due writing from outside the eventloop.
    private final class DefaultHttp2StreamChannel extends DefaultAttributeMap implements Http2StreamChannel {
        private final Http2StreamChannelConfig config = new Http2StreamChannelConfig(this);
        private final Http2ChannelUnsafe unsafe = new Http2ChannelUnsafe();
        private final ChannelId channelId;
        private final ChannelPipeline pipeline;
        private final Http2FrameStream stream;
        private final ChannelPromise closePromise;

        private volatile boolean registered;

        private boolean closePending;
        private boolean readInProgress;
        private Queue<Object> inboundBuffer;

        /** {@code true} after the first HEADERS frame has been written **/
        private boolean firstFrameWritten;

        /** {@code true} if a close without an error was initiated **/
        private boolean streamClosedWithoutError;

        // Keeps track of flush calls in channelReadComplete(...) and aggregate these.
        private boolean inFireChannelReadComplete;
        private boolean flushPending;

        boolean fireChannelReadPending;

        /**
         * The flow control window of the remote side i.e. the number of bytes this channel is allowed to send to the
         * remote peer. The window can become negative if a channel handler ignores the channel's writability. We are
         * using a long so that we realistically don't have to worry about underflow.
         */
        @SuppressWarnings("UnusedDeclaration")
        volatile long outboundFlowControlWindow;

        // Holds the reference to the next DefaultHttp2StreamChannel that should be processed in
        // channelReadComplete(...)
        DefaultHttp2StreamChannel next;

        DefaultHttp2StreamChannel(Http2FrameStream stream) {
            this.stream = stream;
            ((Http2MultiplexCodecStream) stream).channel = this;
            pipeline = new DefaultChannelPipeline(this) {
                @Override
                protected void incrementPendingOutboundBytes(long size) {
                    unsafe.incrementOutboundFlowControlWindow(size, true);
                }

                @Override
                protected void decrementPendingOutboundBytes(long size) {
                    unsafe.incrementOutboundFlowControlWindow(-size, true);
                }
            };
            closePromise = pipeline.newPromise();
            channelId = new Http2StreamChannelId(parent().id(), ++idCount);
        }

        @Override
        public Http2FrameStream stream() {
            return stream;
        }

        void streamClosed() {
            streamClosedWithoutError = true;
            if (readInProgress) {
                // Just call closeForcibly() as this will take care of fireChannelInactive().
                unsafe().closeForcibly();
            } else {
                closePending = true;
            }
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
            return !closePromise.isDone();
        }

        @Override
        public boolean isActive() {
            return isOpen();
        }

        @Override
        public boolean isWritable() {
            // TODO: Should we also take the parent channel into account ?
            return isStreamIdValid(stream.id())
                    // So that the channel doesn't become active before the initial flow control window has been set.
                    && outboundFlowControlWindow > 0;
        }

        @Override
        public ChannelId id() {
            return channelId;
        }

        @Override
        public EventLoop eventLoop() {
            return parent().eventLoop();
        }

        @Override
        public Channel parent() {
            return ctx.channel();
        }

        @Override
        public boolean isRegistered() {
            return registered;
        }

        @Override
        public SocketAddress localAddress() {
            return parent().localAddress();
        }

        @Override
        public SocketAddress remoteAddress() {
            return parent().remoteAddress();
        }

        @Override
        public ChannelFuture closeFuture() {
            return closePromise;
        }

        @Override
        public long bytesBeforeUnwritable() {
            return max(0, outboundFlowControlWindow);
        }

        @Override
        public long bytesBeforeWritable() {
            long window = outboundFlowControlWindow;
            return window < 0 ? -window : 0;
        }

        @Override
        public Unsafe unsafe() {
            return unsafe;
        }

        @Override
        public ChannelPipeline pipeline() {
            return pipeline;
        }

        @Override
        public ByteBufAllocator alloc() {
            return config().getAllocator();
        }

        @Override
        public Channel read() {
            pipeline().read();
            return this;
        }

        @Override
        public Channel flush() {
            pipeline().flush();
            return this;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return pipeline().bind(localAddress);
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return pipeline().connect(remoteAddress);
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
            return pipeline().connect(remoteAddress, localAddress);
        }

        @Override
        public ChannelFuture disconnect() {
            return pipeline().disconnect();
        }

        @Override
        public ChannelFuture close() {
            return pipeline().close();
        }

        @Override
        public ChannelFuture deregister() {
            return pipeline().deregister();
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
            return pipeline().bind(localAddress, promise);
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
            return pipeline().connect(remoteAddress, promise);
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
            return pipeline().connect(remoteAddress, localAddress, promise);
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise promise) {
            return pipeline().disconnect(promise);
        }

        @Override
        public ChannelFuture close(ChannelPromise promise) {
            return pipeline().close(promise);
        }

        @Override
        public ChannelFuture deregister(ChannelPromise promise) {
            return pipeline().deregister(promise);
        }

        @Override
        public ChannelFuture write(Object msg) {
            return pipeline().write(msg);
        }

        @Override
        public ChannelFuture write(Object msg, ChannelPromise promise) {
            return pipeline().write(msg, promise);
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return pipeline().writeAndFlush(msg, promise);
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            return pipeline().writeAndFlush(msg);
        }

        @Override
        public ChannelPromise newPromise() {
            return pipeline().newPromise();
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise() {
            return pipeline().newProgressivePromise();
        }

        @Override
        public ChannelFuture newSucceededFuture() {
            return pipeline().newSucceededFuture();
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable cause) {
            return pipeline().newFailedFuture(cause);
        }

        @Override
        public ChannelPromise voidPromise() {
            return pipeline().voidPromise();
        }

        /**
         * Returns the ID of this channel.
         */
        @Override
        public int hashCode() {
            return id().hashCode();
        }

        /**
         * Returns {@code true} if and only if the specified object is identical
         * with this channel (i.e: {@code this == o}).
         */
        @Override
        public boolean equals(Object o) {
            return this == o;
        }

        @Override
        public int compareTo(Channel o) {
            if (this == o) {
                return 0;
            }

            return id().compareTo(o.id());
        }

        @Override
        public String toString() {
            return parent().toString() + "(HTTP/2 - " + stream + ')';
        }

        void setWritable() {
            assert eventLoop().inEventLoop();
            unsafe.setWritable();
        }

        private Queue<Object> inboundBuffer() {
            if (inboundBuffer == null) {
                inboundBuffer = new ArrayDeque<Object>(4);
            }
            return inboundBuffer;
        }

        /**
         * Receive a read message. This does not notify handlers unless a read is in progress on the
         * channel.
         */
        boolean fireChildRead(Http2Frame frame) {
            assert eventLoop().inEventLoop();
            if (!isActive()) {
                ReferenceCountUtil.release(frame);
            } else {
                if (readInProgress) {
                    assert inboundBuffer == null || inboundBuffer.isEmpty();
                    // Check for null because inboundBuffer doesn't support null; we want to be consistent
                    // for what values are supported.
                    RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
                    unsafe.doRead0(frame, allocHandle);
                    return allocHandle.continueReading();
                } else {
                    inboundBuffer().add(frame);
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

        private final class Http2ChannelUnsafe implements Unsafe {
            private final VoidChannelPromise unsafeVoidPromise =
                    new VoidChannelPromise(DefaultHttp2StreamChannel.this, false);
            @SuppressWarnings("deprecation")
            private RecvByteBufAllocator.Handle recvHandle;
            private boolean flush;
            private boolean writable;
            private boolean inClose;

            void setWritable() {
                assert !isWritable();
                incrementOutboundFlowControlWindow(initialOutboundStreamWindow, false);
                writable = true;
                assert isWritable();
                pipeline().fireChannelWritabilityChanged();
            }

            void incrementOutboundFlowControlWindow(long bytes, boolean mayChangeWritability) {
                if (bytes == 0) {
                    return;
                }
                long window = OUTBOUND_FLOW_CONTROL_WINDOW_UPDATER.addAndGet(DefaultHttp2StreamChannel.this, bytes);
                if (mayChangeWritability) {
                    if (!writable) {
                        if (window > 0) {
                            writable = true;
                            pipeline().fireChannelWritabilityChanged();
                        }
                    } else if (window <= 0) {
                        writable = false;
                        pipeline().fireChannelWritabilityChanged();
                    }
                }
            }

            @Override
            public void connect(final SocketAddress remoteAddress,
                                SocketAddress localAddress, final ChannelPromise promise) {
                promise.setFailure(new UnsupportedOperationException());
            }

            @SuppressWarnings("deprecation")
            @Override
            public RecvByteBufAllocator.Handle recvBufAllocHandle() {
                if (recvHandle == null) {
                    recvHandle = config().getRecvByteBufAllocator().newHandle();
                }
                return recvHandle;
            }

            @Override
            public SocketAddress localAddress() {
                return parent().unsafe().localAddress();
            }

            @Override
            public SocketAddress remoteAddress() {
                return parent().unsafe().remoteAddress();
            }

            @Override
            public void register(EventLoop eventLoop, ChannelPromise promise) {
                if (registered) {
                    throw new UnsupportedOperationException("Re-register is not supported");
                }

                registered = true;

                // Add the handler to the pipeline now that we are registered.
                pipeline().addLast(inboundStreamHandler);

                promise.setSuccess();

                pipeline().fireChannelRegistered();
                if (isActive()) {
                    pipeline().fireChannelActive();
                }
            }

            @Override
            public void bind(SocketAddress localAddress, ChannelPromise promise) {
                promise.setFailure(new UnsupportedOperationException());
            }

            @Override
            public void disconnect(ChannelPromise promise) {
                close(promise);
            }

            @Override
            public void close(ChannelPromise promise) {
                if (closePromise.isDone()) {
                    promise.setFailure(new ClosedChannelException());
                    return;
                }
                if (inClose) {
                    return;
                }
                inClose = true;
                try {
                    closePending = false;
                    fireChannelReadPending = false;

                    // Only ever send a reset frame if the connection is still alive as otherwise it makes no sense at
                    // all anyway.
                    if (parent().isActive() && !streamClosedWithoutError && isStreamIdValid(stream().id())) {
                        Http2StreamFrame resetFrame = new DefaultHttp2ResetFrame(Http2Error.CANCEL).stream(stream());
                        write(resetFrame, unsafe().voidPromise());
                        flush();
                    }

                    if (inboundBuffer != null) {
                        for (;;) {
                            Object msg = inboundBuffer.poll();
                            if (msg == null) {
                                break;
                            }
                            ReferenceCountUtil.release(msg);
                        }
                    }

                    pipeline().fireChannelInactive();
                    if (isRegistered()) {
                        deregister(unsafe().voidPromise());
                    }
                    promise.setSuccess();
                    closePromise.setSuccess();
                } finally {
                    inClose = false;
                }
            }

            @Override
            public void closeForcibly() {
                close(unsafe().voidPromise());
            }

            @Override
            public void deregister(ChannelPromise promise) {
                if (registered) {
                    registered = true;
                    promise.setSuccess();
                    pipeline().fireChannelUnregistered();
                } else {
                    promise.setFailure(new IllegalStateException("Not registered"));
                }
            }

            @Override
            public void beginRead() {
                if (readInProgress || !isActive()) {
                    return;
                }

                final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
                allocHandle.reset(config());
                if (inboundBuffer == null) {
                    if (closePending) {
                        unsafe.closeForcibly();
                    } else {
                        readInProgress = true;
                    }
                    return;
                }

                do {
                    Object m = inboundBuffer.poll();
                    if (m == null) {
                        if (closePending) {
                            pipeline().fireChannelReadComplete();
                            unsafe.closeForcibly();
                            return;
                        }
                    }
                    doRead0((Http2Frame) m, allocHandle);
                } while (allocHandle.continueReading());

                allocHandle.readComplete();
                pipeline().fireChannelReadComplete();
            }

            /**
             * Returns whether reads should continue. The only reason reads shouldn't continue is that the
             * channel was just closed.
             */
            @SuppressWarnings("deprecation")
            void doRead0(Http2Frame frame, RecvByteBufAllocator.Handle allocHandle) {
                int numBytesToBeConsumed = 0;
                if (frame instanceof Http2HeadersFrame) {
                    allocHandle.lastBytesRead(ARBITRARY_MESSAGE_SIZE);
                } else if (frame instanceof Http2DataFrame) {
                    numBytesToBeConsumed = ((Http2DataFrame) frame).flowControlledBytes();
                    allocHandle.lastBytesRead(numBytesToBeConsumed);
                } else {
                    if (frame instanceof Http2WindowUpdateFrame) {
                        Http2WindowUpdateFrame windowUpdate = (Http2WindowUpdateFrame) frame;
                        incrementOutboundFlowControlWindow(windowUpdate.windowSizeIncrement(), true);
                    }
                    allocHandle.lastBytesRead(ARBITRARY_MESSAGE_SIZE);
                }
                allocHandle.incMessagesRead(1);
                pipeline().fireChannelRead(frame);

                if (numBytesToBeConsumed != 0) {
                    try {
                        onBytesConsumed(ctx, stream, numBytesToBeConsumed);
                    } catch (Http2Exception e) {
                        pipeline().fireExceptionCaught(e);
                    }
                }
            }

            @Override
            public void write(Object msg, final ChannelPromise promise) {
                if (!isActive()) {
                    ReferenceCountUtil.release(msg);
                    promise.setFailure(CLOSED_CHANNEL_EXCEPTION);
                    return;
                }

                // After this point its not possible to cancel a write anymore.
                if (!promise.setUncancellable()) {
                    ReferenceCountUtil.release(msg);
                    return;
                }

                final int bytes = FlowControlledFrameSizeEstimator.HANDLE_INSTANCE.size(msg);

                incrementOutboundFlowControlWindow(-bytes, true);

                // TODO(buchgr): Should we also the change the writability if END_STREAM is set?
                try {
                    if (msg instanceof Http2StreamFrame) {
                        Http2StreamFrame frame = validateStreamFrame((Http2StreamFrame) msg).stream(stream());

                        if (!firstFrameWritten && !isStreamIdValid(stream().id())) {
                            if (!(frame instanceof Http2HeadersFrame)) {
                                ReferenceCountUtil.release(frame);
                                promise.setFailure(
                                        new IllegalArgumentException("The first frame must be a headers frame. Was: "
                                        + frame.name()));
                                return;
                            }
                            firstFrameWritten = true;
                            ChannelFuture future = write0(frame);
                            if (future.isDone()) {
                                firstWriteComplete(future, promise);
                            } else {
                                firstWriteComplete(future, promise);
                            }
                            return;
                        }
                    } else if (!(msg instanceof Http2GoAwayFrame)) {
                        String msgStr = msg.toString();
                        ReferenceCountUtil.release(msg);
                        promise.setFailure(new IllegalArgumentException(
                                "Message must be an Http2GoAwayFrame or Http2StreamFrame: " + msgStr));
                        return;
                    }

                    ChannelFuture future = write0(msg);
                    if (future.isDone()) {
                        writeComplete(future, promise, bytes);
                    } else {
                        future.addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                writeComplete(future, promise, bytes);
                            }
                        });
                    }
                } catch (Throwable t) {
                    promise.tryFailure(t);
                } finally {
                    flush = true;
                }
            }

            private void firstWriteComplete(ChannelFuture future, ChannelPromise promise) {
                Throwable cause = future.cause();
                if (cause == null) {
                    promise.setSuccess();
                    setWritable();
                } else {
                    promise.setFailure(cause);
                    closeForcibly();
                }
            }

            private void writeComplete(ChannelFuture future, ChannelPromise promise, long bytes) {
                Throwable cause = future.cause();
                if (cause == null) {
                    promise.setSuccess();
                } else {
                    promise.setFailure(cause);
                    if (bytes > 0 && isActive()) {
                        /*
                         * Return the flow control window of the failed data frame.
                         */
                        incrementOutboundFlowControlWindow(bytes, true);
                    }
                }
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

            private ChannelFuture write0(Object msg) {
                ChannelPromise promise = ctx.newPromise();
                Http2MultiplexCodec.this.write(ctx, msg, promise);
                return promise;
            }

            @Override
            public void flush() {
                if (flush) {
                    // If we are current channelReadComplete(...) call we should just mark this Channel with a flush
                    // pending. We will ensure we trigger ctx.flush() after we processed all Channels later on and so
                    // aggregate the flushes. This is done as ctx.flush() is expensive when as it may trigger an
                    // write(...) or writev(...) operation on the socket.
                    if (inFireChannelReadComplete) {
                        flushPending = true;
                    } else {
                        flush0(ctx);
                    }
                } else {
                    // There is nothing to flush so this is a NOOP.
                }
            }

            @Override
            public ChannelPromise voidPromise() {
                return unsafeVoidPromise;
            }

            @Override
            public ChannelOutboundBuffer outboundBuffer() {
                // Always return null as we not use the ChannelOutboundBuffer and not even support it.
                return null;
            }
        }

        /**
         * {@link ChannelConfig} so that the high and low writebuffer watermarks can reflect the outbound flow control
         * window, without having to create a new {@link WriteBufferWaterMark} object whenever the flow control window
         * changes.
         */
        private final class Http2StreamChannelConfig extends DefaultChannelConfig {

            Http2StreamChannelConfig(Channel channel) {
                super(channel);
                setRecvByteBufAllocator(Http2StreamChannelRecvByteBufAllocator.INSTANCE);
            }

            @Override
            public int getWriteBufferHighWaterMark() {
                return max(0, (int) min(Integer.MAX_VALUE, outboundFlowControlWindow));
            }

            @Override
            public int getWriteBufferLowWaterMark() {
                return getWriteBufferHighWaterMark();
            }

            @Override
            public MessageSizeEstimator getMessageSizeEstimator() {
                return FlowControlledFrameSizeEstimator.INSTANCE;
            }

            @Override
            public WriteBufferWaterMark getWriteBufferWaterMark() {
                int mark = getWriteBufferHighWaterMark();
                return new WriteBufferWaterMark(mark, mark);
            }

            @Override
            public ChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator) {
                throw new UnsupportedOperationException();
            }

            @Override
            @Deprecated
            public ChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
                throw new UnsupportedOperationException();
            }

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
