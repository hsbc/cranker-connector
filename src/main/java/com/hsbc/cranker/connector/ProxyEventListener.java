package com.hsbc.cranker.connector;

import java.net.http.HttpRequest;

/**
 * A listener that gets notified when requests are proxied between a cranker router and a target server
 */
public interface ProxyEventListener {
    /**
     * Called when the request headers are received, before the request is sent to the target server.
     * <p>This callback can be used to inspect or change the request before it is sent to the target
     * server.</p>
     * @param request The request that was prepared to be sent to the target server
     * @param requestBuilder The builder that created the request, which may be used to generate a new
     *                       request if the original request needs to be modified
     * @return The request to use to send to the target server. If no changes were made to the original
     * request then the request passed as the parameter should be returned.
     */
    default HttpRequest beforeProxyToTarget(HttpRequest request, HttpRequest.Builder requestBuilder) {
        return request;
    }

    /**
     * Called when error happen during the request proxying
     * @param request The request that sent (or going to send) to the target server
     * @param error the error
     */
    default void onProxyError(HttpRequest request, Throwable error) {}
}
