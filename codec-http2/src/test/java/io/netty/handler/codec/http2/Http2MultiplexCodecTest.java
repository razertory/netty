/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http2.Http2Exception.StreamException;
import io.netty.util.AsciiString;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.netty.util.ReferenceCountUtil.release;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link Http2MultiplexCodec}.
 */
public class Http2MultiplexCodecTest {

    private EmbeddedChannel parentChannel;
    private Writer writer;

    private TestChannelInitializer childChannelInitializer;

    private static final Http2Headers request = new DefaultHttp2Headers()
            .method(HttpMethod.GET.asciiName()).scheme(HttpScheme.HTTPS.name())
            .authority(new AsciiString("example.org")).path(new AsciiString("/foo"));

    private TestableHttp2MultiplexCodec codec;
    private Http2FrameStream inboundStream;

    private Http2FrameStream outboundStream;

    private static final int initialRemoteStreamWindow = 1024;

    @Before
    public void setUp() {
        childChannelInitializer = new TestChannelInitializer();
        parentChannel = new EmbeddedChannel();
        writer = new Writer();

        parentChannel.connect(new InetSocketAddress(0));
        codec = new TestableHttp2MultiplexCodec(true, childChannelInitializer);
        parentChannel.pipeline().addLast(codec);
        parentChannel.runPendingTasks();

        Http2Settings settings = new Http2Settings().initialWindowSize(initialRemoteStreamWindow);
        codec.onHttp2Frame(new DefaultHttp2SettingsFrame(settings));

        inboundStream = codec.newStream().id(3);
        outboundStream = codec.newStream().id(2);
    }

    @After
    public void tearDown() throws Exception {
        if (childChannelInitializer.handler != null) {
            ((LastInboundHandler) childChannelInitializer.handler).finishAndReleaseAll();
        }
        parentChannel.finishAndReleaseAll();
        codec = null;
    }

    // TODO(buchgr): Flush from child channel
    // TODO(buchgr): ChildChannel.childReadComplete()
    // TODO(buchgr): GOAWAY Logic
    // TODO(buchgr): Test ChannelConfig.setMaxMessagesPerRead

    @Test
    public void headerAndDataFramesShouldBeDelivered() {
        LastInboundHandler inboundHandler = new LastInboundHandler();
        childChannelInitializer.handler = inboundHandler;

        Http2HeadersFrame headersFrame = new DefaultHttp2HeadersFrame(request).stream(inboundStream);
        Http2DataFrame dataFrame1 = new DefaultHttp2DataFrame(bb("hello")).stream(inboundStream);
        Http2DataFrame dataFrame2 = new DefaultHttp2DataFrame(bb("world")).stream(inboundStream);

        assertFalse(inboundHandler.isChannelActive());
        codec.onHttp2StreamActive(inboundStream);
        codec.onHttp2Frame(headersFrame);
        assertTrue(inboundHandler.isChannelActive());
        codec.onHttp2Frame(dataFrame1);
        codec.onHttp2Frame(dataFrame2);

        assertEquals(headersFrame, inboundHandler.readInbound());
        assertEquals(dataFrame1, inboundHandler.readInbound());
        assertEquals(dataFrame2, inboundHandler.readInbound());
        assertNull(inboundHandler.readInbound());

        dataFrame1.release();
        dataFrame2.release();
    }

    @Test
    public void framesShouldBeMultiplexed() {

        Http2FrameStream stream3 = codec.newStream().id(3);
        Http2FrameStream stream5 = codec.newStream().id(5);
        Http2FrameStream stream11 = codec.newStream().id(11);

        LastInboundHandler inboundHandler3 = streamActiveAndWriteHeaders(stream3);
        LastInboundHandler inboundHandler5 = streamActiveAndWriteHeaders(stream5);
        LastInboundHandler inboundHandler11 = streamActiveAndWriteHeaders(stream11);

        verifyFramesMultiplexedToCorrectChannel(stream3, inboundHandler3, 1);
        verifyFramesMultiplexedToCorrectChannel(stream5, inboundHandler5, 1);
        verifyFramesMultiplexedToCorrectChannel(stream11, inboundHandler11, 1);

        codec.onHttp2Frame(new DefaultHttp2DataFrame(bb("hello"), false).stream(stream5));
        codec.onHttp2Frame(new DefaultHttp2DataFrame(bb("foo"), true).stream(stream3));
        codec.onHttp2Frame(new DefaultHttp2DataFrame(bb("world"), true).stream(stream5));
        codec.onHttp2Frame(new DefaultHttp2DataFrame(bb("bar"), true).stream(stream11));
        verifyFramesMultiplexedToCorrectChannel(stream5, inboundHandler5, 2);
        verifyFramesMultiplexedToCorrectChannel(stream3, inboundHandler3, 1);
        verifyFramesMultiplexedToCorrectChannel(stream11, inboundHandler11, 1);
    }

    @Test
    public void inboundDataFrameShouldEmitWindowUpdateFrame() {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);
        ByteBuf tenBytes = bb("0123456789");
        codec.onHttp2Frame(new DefaultHttp2DataFrame(tenBytes, true).stream(inboundStream));
        codec.onChannelReadComplete();

        Http2WindowUpdateFrame windowUpdate = parentChannel.readOutbound();
        assertNotNull(windowUpdate);

        assertEquals(inboundStream, windowUpdate.stream());
        assertEquals(10, windowUpdate.windowSizeIncrement());

        // headers and data frame
        verifyFramesMultiplexedToCorrectChannel(inboundStream, inboundHandler, 2);
    }

    @Test
    public void channelReadShouldRespectAutoRead() {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);
        Channel childChannel = inboundHandler.channel();
        assertTrue(childChannel.config().isAutoRead());
        Http2HeadersFrame headersFrame = inboundHandler.readInbound();
        assertNotNull(headersFrame);

        childChannel.config().setAutoRead(false);
        codec.onHttp2Frame(
                new DefaultHttp2DataFrame(bb("hello world"), false).stream(inboundStream));
        codec.onChannelReadComplete();
        Http2DataFrame dataFrame0 = inboundHandler.readInbound();
        assertNotNull(dataFrame0);
        release(dataFrame0);

        codec.onHttp2Frame(new DefaultHttp2DataFrame(bb("foo"), false).stream(inboundStream));
        codec.onHttp2Frame(new DefaultHttp2DataFrame(bb("bar"), true).stream(inboundStream));
        codec.onChannelReadComplete();

        dataFrame0 = inboundHandler.readInbound();
        assertNull(dataFrame0);

        childChannel.config().setAutoRead(true);
        verifyFramesMultiplexedToCorrectChannel(inboundStream, inboundHandler, 2);
    }

    private Http2StreamChannel newOutboundStream() {
        return new Http2StreamBootstrap(parentChannel).handler(childChannelInitializer)
                .open().syncUninterruptibly().getNow();
    }

    /**
     * A child channel for a HTTP/2 stream in IDLE state (that is no headers sent or received),
     * should not emit a RST_STREAM frame on close, as this is a connection error of type protocol error.
     */

    @Test
    public void idleOutboundStreamShouldNotWriteResetFrameOnClose() {
        childChannelInitializer.handler = new LastInboundHandler();

        Channel childChannel = newOutboundStream();
        assertTrue(childChannel.isActive());

        childChannel.close();
        parentChannel.runPendingTasks();

        assertFalse(childChannel.isOpen());
        assertFalse(childChannel.isActive());
        assertNull(parentChannel.readOutbound());
    }

    @Test
    public void outboundStreamShouldWriteResetFrameOnClose_headersSent() {
        childChannelInitializer.handler = new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                ctx.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
                ctx.fireChannelActive();
            }
        };

        Channel childChannel = newOutboundStream();
        assertTrue(childChannel.isActive());

        Http2FrameStream stream2 = readOutboundHeadersAndAssignId();

        childChannel.close();
        parentChannel.runPendingTasks();

        Http2ResetFrame reset = parentChannel.readOutbound();
        assertEquals(stream2, reset.stream());
        assertEquals(Http2Error.CANCEL.code(), reset.errorCode());
    }

    @Test
    public void inboundRstStreamFireChannelInactive() {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);
        assertTrue(inboundHandler.isChannelActive());
        codec.onHttp2Frame(new DefaultHttp2ResetFrame(Http2Error.INTERNAL_ERROR)
                                                       .stream(inboundStream));
        codec.onChannelReadComplete();

        // This will be called by the frame codec.
        codec.onHttp2StreamClosed(inboundStream);
        parentChannel.runPendingTasks();

        assertFalse(inboundHandler.isChannelActive());
        // A RST_STREAM frame should NOT be emitted, as we received a RST_STREAM.
        assertNull(parentChannel.readOutbound());
    }

    @Test(expected = StreamException.class)
    public void streamExceptionTriggersChildChannelExceptionAndClose() throws Exception {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);

        StreamException cause = new StreamException(inboundStream.id(), Http2Error.PROTOCOL_ERROR, "baaam!");
        Http2FrameStreamException http2Ex = new Http2FrameStreamException(
                inboundStream, Http2Error.PROTOCOL_ERROR, cause);
        codec.onHttp2FrameStreamException(http2Ex);

        inboundHandler.checkException();
    }

    @Test(expected = StreamException.class)
    public void streamExceptionClosesChildChannel() throws Exception {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);

        assertTrue(inboundHandler.isChannelActive());
        StreamException cause = new StreamException(inboundStream.id(), Http2Error.PROTOCOL_ERROR, "baaam!");
        Http2FrameStreamException http2Ex = new Http2FrameStreamException(
                inboundStream, Http2Error.PROTOCOL_ERROR, cause);
        codec.onHttp2FrameStreamException(http2Ex);
        parentChannel.runPendingTasks();

        assertFalse(inboundHandler.isChannelActive());
        inboundHandler.checkException();
    }

    @Test
    public void creatingWritingReadingAndClosingOutboundStreamShouldWork() {
        LastInboundHandler inboundHandler = new LastInboundHandler();
        childChannelInitializer.handler = inboundHandler;

        Http2StreamChannel childChannel = newOutboundStream();
        assertTrue(childChannel.isActive());
        assertTrue(inboundHandler.isChannelActive());

        // Write to the child channel
        Http2Headers headers = new DefaultHttp2Headers().scheme("https").method("GET").path("/foo.txt");
        childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(headers));

        readOutboundHeadersAndAssignId();

        // Read from the child channel
        headers = new DefaultHttp2Headers().scheme("https").status("200");
        codec.onHttp2Frame(new DefaultHttp2HeadersFrame(headers).stream(childChannel.stream()));
        codec.onChannelReadComplete();

        Http2HeadersFrame headersFrame = inboundHandler.readInbound();
        assertNotNull(headersFrame);
        assertSame(headers, headersFrame.headers());

        // Close the child channel.
        childChannel.close();

        parentChannel.runPendingTasks();
        // An active outbound stream should emit a RST_STREAM frame.
        Http2ResetFrame rstFrame = parentChannel.readOutbound();
        assertNotNull(rstFrame);
        assertEquals(childChannel.stream(), rstFrame.stream());
        assertFalse(childChannel.isOpen());
        assertFalse(childChannel.isActive());
        assertFalse(inboundHandler.isChannelActive());
    }

    // Test failing the promise of the first headers frame of an outbound stream. In practice this error case would most
    // likely happen due to the max concurrent streams limit being hit or the channel running out of stream identifiers.
    //
    @Test(expected = Http2NoMoreStreamIdsException.class)
    public void failedOutboundStreamCreationThrowsAndClosesChannel() throws Exception {
        writer = new Writer() {
            @Override
            void write(Object msg, ChannelPromise promise) {
                promise.tryFailure(new Http2NoMoreStreamIdsException());
            }
        };
        LastInboundHandler inboundHandler = new LastInboundHandler();
        childChannelInitializer.handler = inboundHandler;

        Channel childChannel = newOutboundStream();
        assertTrue(childChannel.isActive());

        ChannelFuture future = childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
        parentChannel.flush();

        assertFalse(childChannel.isActive());
        assertFalse(childChannel.isOpen());

        inboundHandler.checkException();

        future.syncUninterruptibly();
    }

    @Test
    public void settingChannelOptsAndAttrs() {
        AttributeKey<String> key = AttributeKey.newInstance("foo");

        Channel childChannel = newOutboundStream();
        childChannel.config().setAutoRead(false).setWriteSpinCount(1000);
        childChannel.attr(key).set("bar");
        assertFalse(childChannel.config().isAutoRead());
        assertEquals(1000, childChannel.config().getWriteSpinCount());
        assertEquals("bar", childChannel.attr(key).get());
    }

    @Test
    public void outboundStreamShouldWriteGoAwayWithoutReset() {
        childChannelInitializer.handler = new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                ctx.writeAndFlush(new DefaultHttp2GoAwayFrame(Http2Error.NO_ERROR));
                ctx.fireChannelActive();
            }
        };
        Channel childChannel = newOutboundStream();
        assertTrue(childChannel.isActive());

        Http2GoAwayFrame goAwayFrame = parentChannel.readOutbound();
        assertNotNull(goAwayFrame);
        goAwayFrame.release();

        childChannel.close();
        parentChannel.runPendingTasks();

        Http2ResetFrame reset = parentChannel.readOutbound();
        assertNull(reset);
    }

    @Test
    public void outboundFlowControlWindowShouldBeSetAndUpdated() {
        Http2StreamChannel childChannel = newOutboundStream();
        assertTrue(childChannel.isActive());

        assertEquals(0, childChannel.config().getWriteBufferHighWaterMark());
        childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
        parentChannel.flush();

        Http2FrameStream stream2 = readOutboundHeadersAndAssignId();

        // Test for initial window size
        assertEquals(initialRemoteStreamWindow, childChannel.config().getWriteBufferHighWaterMark());

        // Test for increment via WINDOW_UPDATE
        codec.onHttp2Frame(new DefaultHttp2WindowUpdateFrame(1).stream(stream2));
        codec.onChannelReadComplete();

        assertEquals(initialRemoteStreamWindow + 1, childChannel.config().getWriteBufferHighWaterMark());
    }

    @Test
    public void onlyDataFramesShouldBeFlowControlled() {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);
        Http2StreamChannel childChannel = (Http2StreamChannel) inboundHandler.channel();
        assertTrue(childChannel.isWritable());
        assertEquals(initialRemoteStreamWindow, childChannel.config().getWriteBufferHighWaterMark());

        childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
        assertTrue(childChannel.isWritable());
        assertEquals(initialRemoteStreamWindow, childChannel.config().getWriteBufferHighWaterMark());

        ByteBuf data = Unpooled.buffer(100).writeZero(100);
        childChannel.writeAndFlush(new DefaultHttp2DataFrame(data));
        assertTrue(childChannel.isWritable());
        assertEquals(initialRemoteStreamWindow - 100, childChannel.config().getWriteBufferHighWaterMark());
    }

    @Test
    public void writabilityAndFlowControl() {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);
        Http2StreamChannel childChannel = (Http2StreamChannel) inboundHandler.channel();
        verifyFlowControlWindowAndWritability(childChannel, initialRemoteStreamWindow);
        assertEquals("true", inboundHandler.writabilityStates());

        // HEADERS frames are not flow controlled, so they should not affect the flow control window.
        childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
        verifyFlowControlWindowAndWritability(childChannel, initialRemoteStreamWindow);
        assertEquals("true", inboundHandler.writabilityStates());

        ByteBuf data = Unpooled.buffer(initialRemoteStreamWindow - 1).writeZero(initialRemoteStreamWindow - 1);
        childChannel.writeAndFlush(new DefaultHttp2DataFrame(data));
        verifyFlowControlWindowAndWritability(childChannel, 1);
        assertEquals("true,false,true", inboundHandler.writabilityStates());

        ByteBuf data1 = Unpooled.buffer(100).writeZero(100);
        childChannel.writeAndFlush(new DefaultHttp2DataFrame(data1));
        verifyFlowControlWindowAndWritability(childChannel, -99);
        assertEquals("true,false,true,false", inboundHandler.writabilityStates());

        codec.onHttp2Frame(new DefaultHttp2WindowUpdateFrame(99).stream(inboundStream));
        codec.onChannelReadComplete();
        // the flow control window should be updated, but the channel should still not be writable.
        verifyFlowControlWindowAndWritability(childChannel, 0);
        assertEquals("true,false,true,false", inboundHandler.writabilityStates());

        codec.onHttp2Frame(new DefaultHttp2WindowUpdateFrame(1).stream(inboundStream));
        codec.onChannelReadComplete();
        verifyFlowControlWindowAndWritability(childChannel, 1);
        assertEquals("true,false,true,false,true", inboundHandler.writabilityStates());
    }

    @Test
    public void failedWriteShouldReturnFlowControlWindow() {
        ByteBuf data = Unpooled.buffer().writeZero(initialRemoteStreamWindow);
        final Http2DataFrame frameToCancel = new DefaultHttp2DataFrame(data);
        writer = new Writer() {
            @Override
            void write(Object msg, ChannelPromise promise) {
                if (msg == frameToCancel) {
                    promise.tryFailure(new Throwable());
                } else {
                    super.write(msg, promise);
                }
            }
        };

        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);
        Channel childChannel = inboundHandler.channel();

        childChannel.write(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
        data = Unpooled.buffer().writeZero(initialRemoteStreamWindow / 2);
        childChannel.write(new DefaultHttp2DataFrame(data));
        assertEquals("true", inboundHandler.writabilityStates());

        childChannel.write(frameToCancel);
        assertEquals("true,false", inboundHandler.writabilityStates());
        assertFalse(childChannel.isWritable());
        childChannel.flush();

        assertTrue(childChannel.isWritable());
        assertEquals("true,false,true", inboundHandler.writabilityStates());
    }

    @Test
    public void cancellingWritesBeforeFlush() {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);
        Channel childChannel = inboundHandler.channel();

        Http2HeadersFrame headers1 = new DefaultHttp2HeadersFrame(new DefaultHttp2Headers());
        Http2HeadersFrame headers2 = new DefaultHttp2HeadersFrame(new DefaultHttp2Headers());
        ChannelPromise writePromise = childChannel.newPromise();
        childChannel.write(headers1, writePromise);
        childChannel.write(headers2);
        assertTrue(writePromise.cancel(false));
        childChannel.flush();

        Http2HeadersFrame headers = parentChannel.readOutbound();
        assertSame(headers, headers2);
    }

    private static void verifyFlowControlWindowAndWritability(Http2StreamChannel channel,
                                                              int expectedWindowSize) {
        assertEquals(Math.max(0, expectedWindowSize), channel.config().getWriteBufferHighWaterMark());
        assertEquals(channel.config().getWriteBufferHighWaterMark(), channel.config().getWriteBufferLowWaterMark());
        assertEquals(expectedWindowSize > 0, channel.isWritable());
    }

    private LastInboundHandler streamActiveAndWriteHeaders(Http2FrameStream stream) {
        LastInboundHandler inboundHandler = new LastInboundHandler();
        childChannelInitializer.handler = inboundHandler;
        assertFalse(inboundHandler.isChannelActive());

        codec.onHttp2StreamActive(stream);
        codec.onHttp2Frame(new DefaultHttp2HeadersFrame(request).stream(stream));
        codec.onChannelReadComplete();
        assertTrue(inboundHandler.isChannelActive());

        return inboundHandler;
    }

    private static void verifyFramesMultiplexedToCorrectChannel(Http2FrameStream stream,
                                                                LastInboundHandler inboundHandler,
                                                                int numFrames) {
        for (int i = 0; i < numFrames; i++) {
            Http2StreamFrame frame = inboundHandler.readInbound();
            assertNotNull(frame);
            assertEquals(stream, frame.stream());
            release(frame);
        }
        assertNull(inboundHandler.readInbound());
    }

    private static ByteBuf bb(String s) {
        return ByteBufUtil.writeUtf8(UnpooledByteBufAllocator.DEFAULT, s);
    }

    /**
     * Simulates the frame codec, in first assigning an identifier and the completing the write promise.
     */
    private Http2FrameStream readOutboundHeadersAndAssignId() {
        // Only peek at the frame, so to not complete the promise of the write. We need to first
        // assign a stream identifier, as the frame codec would do.
        Http2HeadersFrame headersFrame = (Http2HeadersFrame) parentChannel.outboundMessages().peek();
        assertNotNull(headersFrame);
        assertNotNull(headersFrame.stream());
        assertFalse(Http2CodecUtil.isStreamIdValid(headersFrame.stream().id()));
        ((Http2MultiplexCodec.Http2MultiplexCodecStream) headersFrame.stream()).id(outboundStream.id());

        // Now read it and complete the write promise.
        assertSame(headersFrame, parentChannel.readOutbound());

        return headersFrame.stream();
    }

    /**
     * This class removes the bits that would transform the frames to bytes and so make it easier to test the actual
     * special handling of the codec.
     */
    private final class TestableHttp2MultiplexCodec extends Http2MultiplexCodec {

        TestableHttp2MultiplexCodec(boolean server, ChannelHandler inboundStreamHandler) {
            super(server, inboundStreamHandler);
        }

        void onHttp2Frame(Http2Frame frame) {
            onHttp2Frame(ctx, frame);
        }

        void onChannelReadComplete() {
            try {
                onChannelReadComplete(ctx);
            } catch (Http2Exception e) {
                throw new IllegalStateException(e);
            }
        }

        void onHttp2StreamActive(Http2FrameStream stream) {
            onHttp2StreamActive(ctx, stream);
        }

        void onHttp2StreamClosed(Http2FrameStream stream) {
            onHttp2StreamClosed(ctx, stream);
        }

        void onHttp2FrameStreamException(Http2FrameStreamException cause) {
            onHttp2FrameStreamException(ctx, cause);
        }

        @Override
        void onBytesConsumed(ChannelHandlerContext ctx, Http2FrameStream stream, int bytes) {
            writer.write(new DefaultHttp2WindowUpdateFrame(bytes).stream(stream), ctx.newPromise());
        }

        @Override
        void write0(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            writer.write(msg, promise);
        }

        @Override
        void flush0(ChannelHandlerContext ctx) {
            // Do nothing
        }
    }

    class Writer {

        void write(Object msg, ChannelPromise promise) {
            parentChannel.outboundMessages().add(msg);
            promise.setSuccess();
        }
    }
}
