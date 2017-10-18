// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.parsec.clients;

import io.netty.channel.Channel;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.handler.AsyncHandlerExtensions;
import org.asynchttpclient.handler.ProgressAsyncHandler;
import org.asynchttpclient.netty.request.NettyRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * {@link AsyncHandler} wrapper that logs connection related information.
 *
 * @param <T> T
 * @author sho
 */
class ParsecAsyncHandlerWrapper<T> implements AsyncHandler<T>, ProgressAsyncHandler<T>, AsyncHandlerExtensions {

    /**
     * Logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ParsecAsyncHandlerWrapper.class);

    /**
     * response builder.
     */
    private final Response.ResponseBuilder builder = new Response.ResponseBuilder();

    /**
     * asyncHandler.
     */
    private final AsyncHandler<T> asyncHandler;

    /**
     * asyncHandler.
     */
    private final ProgressAsyncHandler<T> progressAsyncHandler;

    /**
     * extensions.
     */
    private final AsyncHandlerExtensions extensions;

    /**
     * ning http request.
     */
    private Request ningRequest;

    /**
     * parsec async progress do.
     */
    private ParsecAsyncProgress progress;
    /**
     * retry count.
     */
    private int requestCount;

    /**
     * last resp code.
     */
    private int lastRespCode;

    /**
     * Constructor.
     *
     * @param asyncHandler asyncHandler
     */
    public ParsecAsyncHandlerWrapper(final AsyncHandler<T> asyncHandler, final Request ningRequest) {
        this.asyncHandler = asyncHandler;
        extensions = (asyncHandler instanceof AsyncHandlerExtensions)
            ? (AsyncHandlerExtensions) asyncHandler : null;
        progressAsyncHandler = (asyncHandler instanceof ProgressAsyncHandler)
            ? (ProgressAsyncHandler<T>) asyncHandler : null;
        this.progress = new ParsecAsyncProgress();
        this.ningRequest = ningRequest;
        this.requestCount = 0;
    }

    @Override
    public void onHostnameResolutionAttempt(String name) {
        LOGGER.debug("onHostnameResolutionAttempt: " + System.currentTimeMillis());
        if (extensions != null) {
            extensions.onHostnameResolutionAttempt(name);
        }
    }

    @Override
    public void onHostnameResolutionSuccess(String name, List<InetSocketAddress> addresses) {
        LOGGER.debug("onHostnameResolutionSuccess: " + System.currentTimeMillis());
        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_NAMELOOKUP);
        if (extensions != null) {
            extensions.onHostnameResolutionSuccess(name, addresses);
        }
    }

    @Override
    public void onHostnameResolutionFailure(String name, Throwable cause) {
        LOGGER.debug("onHostnameResolutionFailure: " + System.currentTimeMillis());
        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_NAMELOOKUP);
        if (extensions != null) {
            extensions.onHostnameResolutionFailure(name, cause);
        }
    }

    @Override
    public void onTcpConnectAttempt(InetSocketAddress remoteAddress) {
        LOGGER.debug("onTcpConnectAttempt: " + System.currentTimeMillis());
        if (extensions != null) {
            extensions.onTcpConnectAttempt(remoteAddress);
        }
    }

    @Override
    public void onTcpConnectSuccess(InetSocketAddress remoteAddress, Channel connection) {
        LOGGER.debug("onTcpConnectSuccess: " + System.currentTimeMillis());
        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_CONNECT);
        if (extensions != null) {
            extensions.onTcpConnectSuccess(remoteAddress, connection);
        }
    }

    @Override
    public void onTcpConnectFailure(InetSocketAddress remoteAddress, Throwable cause) {
        LOGGER.debug("onTcpConnectFailure: " + System.currentTimeMillis());
        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_CONNECT);
        if (extensions != null) {
            extensions.onTcpConnectFailure(remoteAddress, cause);
        }
    }

    @Override
    public void onTlsHandshakeAttempt() {
        LOGGER.debug("onTlsHandshakeAttempt: " + System.currentTimeMillis());
        if (extensions != null) {
            extensions.onTlsHandshakeAttempt();
        }
    }

    @Override
    public void onTlsHandshakeSuccess() {
        LOGGER.debug("onTlsHandshakeSuccess: " + System.currentTimeMillis());
        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_TLS);
        if (extensions != null) {
            extensions.onTlsHandshakeSuccess();
        }
    }

    @Override
    public void onTlsHandshakeFailure(Throwable cause) {
        LOGGER.debug("onTlsHandshakeFailure: " + System.currentTimeMillis());
        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_TLS);
        if (extensions != null) {
            extensions.onTlsHandshakeFailure(cause);
        }
    }

    @Override
    public void onConnectionPoolAttempt() {
        LOGGER.debug("onConnectionPoolAttempt: " + System.currentTimeMillis());
        requestCount++;
        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_STARTSINGLE);
        if (extensions != null) {
            extensions.onConnectionPoolAttempt();
        }
    }

    @Override
    public void onConnectionPooled(Channel connection) {
        LOGGER.debug("onConnectionPooled: " + System.currentTimeMillis());
        if (extensions != null) {
            extensions.onConnectionPooled(connection);
        }
    }

    @Override
    public void onConnectionOffer(Channel connection) {
        LOGGER.debug("onConnectionOffer: " + System.currentTimeMillis());
        if (extensions != null) {
            extensions.onConnectionOffer(connection);
        }
    }

    @Override
    public void onRequestSend(NettyRequest request) {
        LOGGER.debug("onRequestSend: " + System.currentTimeMillis());
        if (extensions != null) {
            extensions.onRequestSend(request);
        }
    }

    public void onRetry() {
        LOGGER.debug("onRetry: " + System.currentTimeMillis());
        if (extensions != null) {
            extensions.onRetry();
        }
    }

    @Override
    public State onBodyPartReceived(final HttpResponseBodyPart bodyPart) throws Exception {
        LOGGER.debug("onBodyPartReceived: " + System.currentTimeMillis());
        builder.accumulate(bodyPart);
        return asyncHandler.onBodyPartReceived(bodyPart);
    }

    @Override
    public State onHeadersReceived(final HttpResponseHeaders headers) throws Exception {
        LOGGER.debug("onHeadersReceived: " + System.currentTimeMillis());
        builder.accumulate(headers);
        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_STARTTRANSFER);
        return asyncHandler.onHeadersReceived(headers);
    }

    @Override
    public State onStatusReceived(final HttpResponseStatus responseStatus) throws Exception {
        LOGGER.debug("onStatusReceived: " + System.currentTimeMillis());
        builder.reset();
        builder.accumulate(responseStatus);
        return asyncHandler.onStatusReceived(responseStatus);
    }

    @Override
    public void onThrowable(Throwable t) {
        LOGGER.debug("onThrowable: " + System.currentTimeMillis());
        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_TOTAL);
        writeProfilingLog(null);
        progress.reset();
        lastRespCode = -1;
        asyncHandler.onThrowable(t);
    }

    @Override
    public T onCompleted() throws Exception {
        LOGGER.debug("onCompleted: " + System.currentTimeMillis());
        final Response ningResponse = builder.build();

        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_TOTAL);
        writeProfilingLog(ningResponse);
        progress.reset();
        lastRespCode = ningResponse.getStatusCode();

        return asyncHandler.onCompleted();
    }

    @Override
    public State onHeadersWritten() {
        LOGGER.debug("onHeadersWritten: " + System.currentTimeMillis());
        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_PRETRANSFER);
        if (progressAsyncHandler != null) {
            return progressAsyncHandler.onHeadersWritten();
        }
        return null;
    }

    @Override
    public State onContentWritten() {
        LOGGER.debug("onContentWritten: " + System.currentTimeMillis());
        ParsecAsyncProgressTimer.progressTime(progress, ParsecAsyncProgressTimer.TimerOpCode.TIMER_PRETRANSFER);
        if (progressAsyncHandler != null) {
            return progressAsyncHandler.onContentWritten();
        }
        return null;
    }

    public State onContentWriteProgress(long amount, long current, long total) {
        LOGGER.debug("onContentWriteProgress: " + System.currentTimeMillis());
        if (progressAsyncHandler != null) {
            return progressAsyncHandler.onContentWriteProgress(amount, current, total);
        }
        return null;
    }

    public ParsecAsyncProgress getProgress() {
        return this.progress;
    }

    private void writeProfilingLog(
            final Response ningResponse) {
        String requestStatus = ParsecClientDefine.REQUEST_SINGLE;
        if (requestCount > 1) {
            requestStatus = ParsecClientDefine.REQUEST_SINGLE_RETRY + ":" + lastRespCode;
        }
        ParsecClientProfilingLogUtil.logRemoteRequest(
                ningRequest,
                ningResponse,
                requestStatus,
                progress
        );
    }
}
