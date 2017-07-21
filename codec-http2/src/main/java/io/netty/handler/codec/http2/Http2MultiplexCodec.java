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
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelPromiseNotifier;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.UnstableApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.netty.handler.codec.http2.Http2CodecUtil.isOutboundStream;
import static io.netty.handler.codec.http2.Http2CodecUtil.isStreamIdValid;

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
 * <p>Outbound streams are supported via the {@link #newOutboundStream(ChannelHandler)}.
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
public class Http2MultiplexCodec extends Http2ChannelDuplexHandler {

    private static final ChannelFutureListener CHILD_CHANNEL_REGISTRATION_LISTENER = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            // Handle any errors that occurred on the local thread while registering. Even though
            // failures can happen after this point, they will be handled by the channel by closing the
            // childChannel.
            Channel childChannel = future.channel();
            if (future.cause() != null) {
                if (childChannel.isRegistered()) {
                    childChannel.close();
                } else {
                    childChannel.unsafe().closeForcibly();
                }
            }
        }
    };

    // TODO: Use some sane initial capacity.
    private final Map<Http2FrameStream, Http2StreamChannel> channels =
            new ConcurrentHashMap<Http2FrameStream, Http2StreamChannel>();

    private final List<Http2StreamChannel> channelsToFireChildReadComplete = new ArrayList<Http2StreamChannel>();
    private final boolean server;
    private final ChannelHandler inboundStreamHandler;

    private final ChannelFutureListener firstFrameWriteListener = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            Http2StreamChannel channel = (Http2StreamChannel) future.channel();
            if (future.isSuccess()) {
                Http2FrameStream stream = channel.stream();
                onStreamActive(stream);
            } else {
                channel.pipeline().fireExceptionCaught(future.cause());
                channel.close();
            }
        }
    };

    // Visible for testing
    ChannelHandlerContext ctx;

    private int initialOutboundStreamWindow = Http2CodecUtil.DEFAULT_WINDOW_SIZE;

    /**
     * Construct a new handler whose child channels run in a different event loop.
     *
     * @param server {@code true} this is a server
     */
    public Http2MultiplexCodec(boolean server, ChannelHandler inboundStreamHandler) {
        this.server = server;
        this.inboundStreamHandler = checkSharable(
                ObjectUtil.checkNotNull(inboundStreamHandler, "inboundStreamHandler"));
    }

    private static ChannelHandler checkSharable(ChannelHandler handler) {
        if ((handler instanceof ChannelHandlerAdapter && !((ChannelHandlerAdapter) handler).isSharable()) ||
                !handler.getClass().isAnnotationPresent(Sharable.class)) {
            throw new IllegalArgumentException("The handler must be Sharable");
        }
        return handler;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (ctx.executor() != ctx.channel().eventLoop()) {
            throw new IllegalStateException("EventExecutor must be EventLoop of Channel");
        }
        this.ctx = ctx;
        super.handlerAdded(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        channels.clear();
        channelsToFireChildReadComplete.clear();
        super.handlerRemoved(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof Http2FrameStreamEvent) {
            Http2FrameStreamEvent streamEvt = (Http2FrameStreamEvent) evt;
            switch (streamEvt.state()) {
                case CLOSED:
                    onStreamClosed(streamEvt.stream());
                    break;
                case ACTIVE:
                    onStreamActive(streamEvt.stream());
                    break;
                default:
                    throw new Error();
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof Http2Frame)) {
            ctx.fireChannelRead(msg);
            return;
        }

        if (msg instanceof Http2StreamFrame) {
            channelReadStreamFrame((Http2StreamFrame) msg);
        } else if (msg instanceof Http2GoAwayFrame) {
            final Http2GoAwayFrame goAwayFrame = (Http2GoAwayFrame) msg;
            try {
                forEachActiveStream(new Http2FrameStreamVisitor() {
                    @Override
                    public boolean visit(Http2FrameStream stream) {
                        final int streamId = stream.id();
                        final Http2StreamChannel childChannel = channels.get(stream);
                        if (streamId > goAwayFrame.lastStreamId() && isOutboundStream(server, streamId)) {
                            childChannel.pipeline().fireUserEventTriggered(goAwayFrame.retainedDuplicate());
                        }
                        return true;
                    }
                });
            } finally {
                // We need to ensure we release the goAwayFrame.
                goAwayFrame.release();
            }
        } else if (msg instanceof Http2SettingsFrame) {
            Http2Settings settings = ((Http2SettingsFrame) msg).settings();
            if (settings.initialWindowSize() != null) {
                initialOutboundStreamWindow = settings.initialWindowSize();
            }
        }
    }

    private void channelReadStreamFrame(Http2StreamFrame frame) {
        Http2FrameStream stream = frame.stream();

        Http2StreamChannel childChannel = channels.get(stream);

        // TODO: Should this happen now that onStreamActive(...) is called when an Http2FrameStreamEvent with state
        // ACTIVE is received.
        if (childChannel == null) {
            childChannel = onStreamActive(stream);
        }

        fireChildReadAndRegister(childChannel, frame);
    }

    private void onStreamClosed(Http2FrameStream stream) {
        Http2StreamChannel childChannel = channels.get(stream);
        if (childChannel != null) {
            childChannel.streamClosed();
        }
    }

    private Http2StreamChannel onStreamActive(Http2FrameStream stream) {
        Http2StreamChannel childChannel = channels.get(stream);
        if (childChannel == null) {
            childChannel = newStreamChannel(stream);

            childChannel.pipeline().addLast(inboundStreamHandler);

            ChannelFuture future = ctx.channel().eventLoop().register(childChannel);
            future.addListener(CHILD_CHANNEL_REGISTRATION_LISTENER);
        }

        assert !childChannel.isWritable();
        childChannel.incrementOutboundFlowControlWindow(initialOutboundStreamWindow);
        childChannel.pipeline().fireChannelWritabilityChanged();
        return childChannel;
    }

    // TODO: This is most likely not the best way to expose this, need to think more about it.
    public final ChannelFuture newOutboundStream(ChannelHandler outboundStreamHandler) {
        Http2StreamChannel childChannel = newStreamChannel(newStream());

        if (outboundStreamHandler != null) {
            childChannel.pipeline().addLast(outboundStreamHandler);
        }
        ChannelFuture future = ctx.channel().eventLoop().register(childChannel);
        future.addListener(CHILD_CHANNEL_REGISTRATION_LISTENER);
        return future;
    }

    private Http2StreamChannel newStreamChannel(Http2FrameStream stream) {
        Http2StreamChannel childChannel = new Http2StreamChannel(ctx, stream, firstFrameWriteListener);
        channels.put(stream, childChannel);
        return childChannel;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof Http2FrameStreamException) {
            Http2FrameStreamException streamException = (Http2FrameStreamException) cause;
            Http2FrameStream stream = streamException.stream();
            Http2StreamChannel childChannel = channels.get(stream);

            try {
                childChannel.pipeline().fireExceptionCaught(streamException.getCause());
            } finally {
                childChannel.close();
            }
        } else {
            ctx.fireExceptionCaught(cause);
        }
    }

    private void fireChildReadAndRegister(Http2StreamChannel childChannel, Http2StreamFrame frame) {
        // Can't use childChannel.fireChannelRead() as it would fire independent of whether
        // channel.read() had been called.
        childChannel.fireChildRead(frame);
        if (!childChannel.inStreamsToFireChildReadComplete) {
            channelsToFireChildReadComplete.add(childChannel);
            childChannel.inStreamsToFireChildReadComplete = true;
        }
    }

    /**
     * Notifies any child streams of the read completion.
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        for (int i = 0; i < channelsToFireChildReadComplete.size(); i++) {
            Http2StreamChannel childChannel = channelsToFireChildReadComplete.get(i);
            // Clear early in case fireChildReadComplete() causes it to need to be re-processed
            childChannel.inStreamsToFireChildReadComplete = false;
            childChannel.fireChildReadComplete();
        }
        channelsToFireChildReadComplete.clear();
    }

    private static final class Http2StreamChannel extends AbstractHttp2StreamChannel {

        private final ChannelHandlerContext ctx;
        private final ChannelFutureListener firstFrameWriteListener;

        /** {@code true} after the first HEADERS frame has been written **/
        private boolean firstFrameWritten;

        /** {@code true} if a close without an error was initiated **/
        private boolean streamClosedWithoutError;

        /** {@code true} if stream is in {@link Http2MultiplexCodec#channelsToFireChildReadComplete}. **/
        boolean inStreamsToFireChildReadComplete;

        Http2StreamChannel(ChannelHandlerContext ctx, Http2FrameStream stream,
                           ChannelFutureListener firstFrameWriteListener) {
            super(ctx.channel(), stream);
            this.ctx = ctx;
            this.firstFrameWriteListener = firstFrameWriteListener;
        }

        void streamClosed() {
            streamClosedWithoutError = true;
            fireChildRead(AbstractHttp2StreamChannel.CLOSE_MESSAGE);
        }

        @Override
        protected void doClose() throws Exception {
            if (!streamClosedWithoutError && isStreamIdValid(stream().id())) {
                Http2StreamFrame resetFrame = new DefaultHttp2ResetFrame(Http2Error.CANCEL).stream(stream());
                ctx.writeAndFlush(resetFrame);
            }
            super.doClose();
        }

        @Override
        protected void doWrite(Object msg, ChannelPromise childPromise) {
            if (msg instanceof Http2StreamFrame) {
                Http2StreamFrame frame = validateStreamFrame(msg);
                if (!firstFrameWritten && !isStreamIdValid(stream().id())) {
                    if (!(frame instanceof Http2HeadersFrame)) {
                        throw new IllegalArgumentException("The first frame must be a headers frame. Was: "
                                + frame.name());
                    }
                    childPromise.addListener(firstFrameWriteListener);
                    firstFrameWritten = true;
                }
                frame.stream(stream());

                /**
                 * Wrap the ChannelPromise of the child channel in a ChannelPromise of the parent channel
                 * in order to be able to use it on the parent channel. We don't need to worry about the
                 * channel being cancelled, as the outbound buffer of the child channel marks it uncancelable.
                 */
                assert !childPromise.isCancellable();
                ChannelFutureListener childPromiseNotifier = new ChannelPromiseNotifier(childPromise);
                ctx.write(frame).addListener(childPromiseNotifier);
            } else if (msg instanceof Http2GoAwayFrame) {
                ctx.write(msg);
            } else {
                String msgStr = msg.toString();
                ReferenceCountUtil.release(msg);
                throw new IllegalArgumentException(
                        "Message must be an Http2GoAwayFrame or Http2StreamFrame: " + msgStr);
            }
        }

        @Override
        protected void doWriteComplete() {
            ctx.flush();
        }

        @Override
        protected void bytesConsumed(final int bytes) {
            ctx.write(new DefaultHttp2WindowUpdateFrame(bytes).stream(stream()));
        }

        private static Http2StreamFrame validateStreamFrame(Object msg) {
            if (!(msg instanceof Http2StreamFrame)) {
                String msgString = msg.toString();
                ReferenceCountUtil.release(msg);
                throw new IllegalArgumentException("Message must be a Http2StreamFrame: " + msgString);
            }
            Http2StreamFrame frame = (Http2StreamFrame) msg;
            if (frame.stream() != null) {
                String msgString = msg.toString();
                ReferenceCountUtil.release(frame);
                throw new IllegalArgumentException("Stream must not be set on the frame: " + msgString);
            }
            return frame;
        }
    }
}
