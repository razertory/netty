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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http2.Http2Exception.StreamException;
import io.netty.handler.codec.http2.Http2Stream.State;
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

    private TestChannelInitializer childChannelInitializer;

    private static final Http2Headers request = new DefaultHttp2Headers()
            .method(HttpMethod.GET.asciiName()).scheme(HttpScheme.HTTPS.name())
            .authority(new AsciiString("example.org")).path(new AsciiString("/foo"));

    private Http2FrameStream inboundStream;

    private Http2FrameStream outboundStream;

    private static final int initialRemoteStreamWindow = 1024;

    @Before
    public void setUp() {
        childChannelInitializer = new TestChannelInitializer();
        parentChannel = new EmbeddedChannel();
        parentChannel.connect(new InetSocketAddress(0));
        parentChannel.pipeline().addLast(new TestableHttp2MultiplexCodec(true, childChannelInitializer));
        parentChannel.runPendingTasks();

        Http2Settings settings = new Http2Settings().initialWindowSize(initialRemoteStreamWindow);
        parentChannel.pipeline().fireChannelRead(new DefaultHttp2SettingsFrame(settings));

        inboundStream = new TestHttp2FrameStream().id(3);
        outboundStream = new TestHttp2FrameStream().id(2);
    }

    @After
    public void tearDown() throws Exception {
        if (childChannelInitializer.handler != null) {
            ((LastInboundHandler) childChannelInitializer.handler).finishAndReleaseAll();
        }
        parentChannel.finishAndReleaseAll();
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
        parentChannel.pipeline().fireChannelRead(headersFrame);
        assertTrue(inboundHandler.isChannelActive());
        parentChannel.pipeline().fireChannelRead(dataFrame1);
        parentChannel.pipeline().fireChannelRead(dataFrame2);

        assertEquals(headersFrame, inboundHandler.readInbound());
        assertEquals(dataFrame1, inboundHandler.readInbound());
        assertEquals(dataFrame2, inboundHandler.readInbound());
        assertNull(inboundHandler.readInbound());

        dataFrame1.release();
        dataFrame2.release();
    }

    @Test
    public void framesShouldBeMultiplexed() {

        Http2FrameStream stream3 = new TestHttp2FrameStream().id(3);
        Http2FrameStream stream5 = new TestHttp2FrameStream().id(5);
        Http2FrameStream stream11 = new TestHttp2FrameStream().id(11);

        LastInboundHandler inboundHandler3 = streamActiveAndWriteHeaders(stream3);
        LastInboundHandler inboundHandler5 = streamActiveAndWriteHeaders(stream5);
        LastInboundHandler inboundHandler11 = streamActiveAndWriteHeaders(stream11);

        verifyFramesMultiplexedToCorrectChannel(stream3, inboundHandler3, 1);
        verifyFramesMultiplexedToCorrectChannel(stream5, inboundHandler5, 1);
        verifyFramesMultiplexedToCorrectChannel(stream11, inboundHandler11, 1);

        parentChannel.pipeline().fireChannelRead(new DefaultHttp2DataFrame(bb("hello"), false).stream(stream5));
        parentChannel.pipeline().fireChannelRead(new DefaultHttp2DataFrame(bb("foo"), true).stream(stream3));
        parentChannel.pipeline().fireChannelRead(new DefaultHttp2DataFrame(bb("world"), true).stream(stream5));
        parentChannel.pipeline().fireChannelRead(new DefaultHttp2DataFrame(bb("bar"), true).stream(stream11));
        verifyFramesMultiplexedToCorrectChannel(stream5, inboundHandler5, 2);
        verifyFramesMultiplexedToCorrectChannel(stream3, inboundHandler3, 1);
        verifyFramesMultiplexedToCorrectChannel(stream11, inboundHandler11, 1);
    }

    @Test
    public void inboundDataFrameShouldEmitWindowUpdateFrame() {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);
        ByteBuf tenBytes = bb("0123456789");
        parentChannel.pipeline().fireChannelRead(
                new DefaultHttp2DataFrame(tenBytes, true).stream(inboundStream));
        parentChannel.pipeline().fireChannelReadComplete();

        // Flush is only necessary cause of EmbeddedChannel
        parentChannel.flush();
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
        parentChannel.pipeline().fireChannelRead(
                new DefaultHttp2DataFrame(bb("hello world"), false).stream(inboundStream));
        parentChannel.pipeline().fireChannelReadComplete();
        Http2DataFrame dataFrame0 = inboundHandler.readInbound();
        assertNotNull(dataFrame0);
        release(dataFrame0);

        parentChannel.pipeline().fireChannelRead(new DefaultHttp2DataFrame(bb("foo"), false).stream(inboundStream));
        parentChannel.pipeline().fireChannelRead(new DefaultHttp2DataFrame(bb("bar"), true).stream(inboundStream));
        parentChannel.pipeline().fireChannelReadComplete();

        dataFrame0 = inboundHandler.readInbound();
        assertNull(dataFrame0);

        childChannel.config().setAutoRead(true);
        verifyFramesMultiplexedToCorrectChannel(inboundStream, inboundHandler, 2);
    }

    private Channel newOutboundStream() {
        return parentChannel.pipeline().get(Http2MultiplexCodec.class)
                .newOutboundStream(childChannelInitializer).syncUninterruptibly().channel();
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

        parentChannel.flush();

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
        parentChannel.pipeline().fireChannelRead(new DefaultHttp2ResetFrame(Http2Error.INTERNAL_ERROR)
                                                       .stream(inboundStream));
        parentChannel.pipeline().fireChannelReadComplete();

        // This will be called by the frame codec.
        parentChannel.pipeline().fireUserEventTriggered(Http2FrameStreamEvent.closed(inboundStream));
        parentChannel.runPendingTasks();

        assertFalse(inboundHandler.isChannelActive());
        // A RST_STREAM frame should NOT be emitted, as we received a RST_STREAM.
        assertNull(parentChannel.readOutbound());
    }

    @Test(expected = StreamException.class)
    public void streamExceptionTriggersChildChannelExceptionAndClose() throws Exception {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);

        StreamException cause = new StreamException(inboundStream.id(), Http2Error.PROTOCOL_ERROR, "baaam!");
        Exception http2Ex = new Http2FrameStreamException(inboundStream, Http2Error.PROTOCOL_ERROR, cause);
        parentChannel.pipeline().fireExceptionCaught(http2Ex);

        inboundHandler.checkException();
    }

    @Test(expected = StreamException.class)
    public void streamExceptionClosesChildChannel() throws Exception {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);

        assertTrue(inboundHandler.isChannelActive());
        StreamException cause = new StreamException(inboundStream.id(), Http2Error.PROTOCOL_ERROR, "baaam!");
        Exception http2Ex = new Http2FrameStreamException(inboundStream, Http2Error.PROTOCOL_ERROR, cause);
        parentChannel.pipeline().fireExceptionCaught(http2Ex);
        parentChannel.runPendingTasks();

        assertFalse(inboundHandler.isChannelActive());
        inboundHandler.checkException();
    }

    @Test
    public void creatingWritingReadingAndClosingOutboundStreamShouldWork() {
        LastInboundHandler inboundHandler = new LastInboundHandler();
        childChannelInitializer.handler = inboundHandler;

        Http2MultiplexCodec.Http2StreamChannel childChannel =
                (Http2MultiplexCodec.Http2StreamChannel) newOutboundStream();
        assertTrue(childChannel.isActive());
        assertTrue(inboundHandler.isChannelActive());

        // Write to the child channel
        Http2Headers headers = new DefaultHttp2Headers().scheme("https").method("GET").path("/foo.txt");
        childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(headers));

        readOutboundHeadersAndAssignId();

        // Read from the child channel
        headers = new DefaultHttp2Headers().scheme("https").status("200");
        parentChannel.pipeline().fireChannelRead(
                new DefaultHttp2HeadersFrame(headers).stream(childChannel.stream()));
        parentChannel.pipeline().fireChannelReadComplete();

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
        parentChannel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                promise.tryFailure(new Http2NoMoreStreamIdsException());
            }
        });

        LastInboundHandler inboundHandler = new LastInboundHandler();
        childChannelInitializer.handler = inboundHandler;

        Channel childChannel = newOutboundStream();
        assertTrue(childChannel.isActive());

        childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
        parentChannel.flush();

        assertFalse(childChannel.isActive());
        assertFalse(childChannel.isOpen());

        inboundHandler.checkException();
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
        Http2MultiplexCodec.Http2StreamChannel childChannel =
                (Http2MultiplexCodec.Http2StreamChannel) newOutboundStream();
        assertTrue(childChannel.isActive());

        assertEquals(0, childChannel.getOutboundFlowControlWindow());
        childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
        parentChannel.flush();

        Http2FrameStream stream2 = readOutboundHeadersAndAssignId();

        // Test for initial window size
        assertEquals(initialRemoteStreamWindow, childChannel.getOutboundFlowControlWindow());

        // Test for increment via WINDOW_UPDATE
        parentChannel.pipeline().fireChannelRead(new DefaultHttp2WindowUpdateFrame(1).stream(stream2));
        parentChannel.pipeline().fireChannelReadComplete();

        assertEquals(initialRemoteStreamWindow + 1, childChannel.getOutboundFlowControlWindow());
    }

    @Test
    public void onlyDataFramesShouldBeFlowControlled() {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);
        Http2MultiplexCodec.Http2StreamChannel childChannel =
                (Http2MultiplexCodec.Http2StreamChannel) inboundHandler.channel();
        assertTrue(childChannel.isWritable());
        assertEquals(initialRemoteStreamWindow, childChannel.getOutboundFlowControlWindow());

        childChannel.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers()));
        assertTrue(childChannel.isWritable());
        assertEquals(initialRemoteStreamWindow, childChannel.getOutboundFlowControlWindow());

        ByteBuf data = Unpooled.buffer(100).writeZero(100);
        childChannel.writeAndFlush(new DefaultHttp2DataFrame(data));
        assertTrue(childChannel.isWritable());
        assertEquals(initialRemoteStreamWindow - 100, childChannel.getOutboundFlowControlWindow());
    }

    @Test
    public void writabilityAndFlowControl() {
        LastInboundHandler inboundHandler = streamActiveAndWriteHeaders(inboundStream);
        Http2MultiplexCodec.Http2StreamChannel childChannel =
                (Http2MultiplexCodec.Http2StreamChannel) inboundHandler.channel();
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

        parentChannel.pipeline().fireChannelRead(new DefaultHttp2WindowUpdateFrame(99).stream(inboundStream));
        parentChannel.pipeline().fireChannelReadComplete();
        // the flow control window should be updated, but the channel should still not be writable.
        verifyFlowControlWindowAndWritability(childChannel, 0);
        assertEquals("true,false,true,false", inboundHandler.writabilityStates());

        parentChannel.pipeline().fireChannelRead(new DefaultHttp2WindowUpdateFrame(1).stream(inboundStream));
        parentChannel.pipeline().fireChannelReadComplete();
        verifyFlowControlWindowAndWritability(childChannel, 1);
        assertEquals("true,false,true,false,true", inboundHandler.writabilityStates());
    }

    @Test
    public void failedWriteShouldReturnFlowControlWindow() {
        ByteBuf data = Unpooled.buffer().writeZero(initialRemoteStreamWindow);
        final Http2DataFrame frameToCancel = new DefaultHttp2DataFrame(data);
        parentChannel.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                if (msg == frameToCancel) {
                    promise.tryFailure(new Throwable());
                } else {
                    super.write(ctx, msg, promise);
                }
            }
        });

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

    private static void verifyFlowControlWindowAndWritability(Http2MultiplexCodec.Http2StreamChannel channel,
                                                              int expectedWindowSize) {
        assertEquals(expectedWindowSize, channel.getOutboundFlowControlWindow());
        assertEquals(Math.max(0, expectedWindowSize), channel.config().getWriteBufferHighWaterMark());
        assertEquals(channel.config().getWriteBufferHighWaterMark(), channel.config().getWriteBufferLowWaterMark());
        assertEquals(expectedWindowSize > 0, channel.isWritable());
    }

    private LastInboundHandler streamActiveAndWriteHeaders(Http2FrameStream stream) {
        LastInboundHandler inboundHandler = new LastInboundHandler();
        childChannelInitializer.handler = inboundHandler;
        assertFalse(inboundHandler.isChannelActive());

        parentChannel.pipeline().fireChannelRead(new DefaultHttp2HeadersFrame(request).stream(stream));
        parentChannel.pipeline().fireChannelReadComplete();
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
        ((TestHttp2FrameStream) headersFrame.stream()).id(outboundStream.id());

        // Now read it and complete the write promise.
        assertSame(headersFrame, parentChannel.readOutbound());

        return headersFrame.stream();
    }

    /**
     * This class removes the bits that would require the frame codec, so that the class becomes testable.
     */
    private static final class TestableHttp2MultiplexCodec extends Http2MultiplexCodec {

        TestableHttp2MultiplexCodec(boolean server, ChannelHandler inboundStreamHandler) {
            super(server, inboundStreamHandler);
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            this.ctx = ctx;
        }

        @Override
        void forEachActiveStream0(Http2FrameStreamVisitor streamVisitor) {
            throw new UnsupportedOperationException();
        }

        @Override
        Http2FrameStream newStream0() {
            return new TestHttp2FrameStream();
        }
    }

    private static final class TestHttp2FrameStream implements Http2FrameStream {

        private volatile int id = -1;

        Http2FrameStream id(int id) {
            this.id = id;
            return this;
        }

        @Override
        public int id() {
            return id;
        }

        @Override
        public State state() {
            return State.OPEN;
        }
    }
}
