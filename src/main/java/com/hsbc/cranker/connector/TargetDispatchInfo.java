package com.hsbc.cranker.connector;

import java.net.http.HttpRequest;
import java.util.Objects;

/**
 * Read-only information for inspecting a prepared target request before the proxy listener modifies it.
 * Do not subscribe to or consume the request's body publisher; it belongs to the target HTTP client.
 */
public final class TargetDispatchInfo {
    private final HttpRequest request;

    TargetDispatchInfo(HttpRequest request) {
        this.request = Objects.requireNonNull(request, "request");
    }

    /**
     * Gets the immutable prepared request, before {@link ProxyEventListener#beforeProxyToTarget}
     * is invoked. All instrumentation listeners for an invocation inspect the same request.
     *
     * @return The prepared request, for inspection only
     */
    public HttpRequest request() {
        return request;
    }
}
