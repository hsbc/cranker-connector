package com.hsbc.cranker.connector;

/**
 * Optional hooks for tracing and other instrumentation around requests.
 * <p>Most applications should use {@link ProxyEventListener} instead, to inspect or modify
 * outgoing requests or receive proxy-error notifications. This interface is for integrations
 * that need temporary thread context and cleanup around synchronous request submission,
 * such as making a tracing parent current for HTTP-client instrumentation or setting and
 * restoring a logging context.</p>
 * <p>Instrumentation neither changes the request nor requires a particular instrumentation
 * framework. Callbacks are optional; override only those needed by the integration.
 * Register implementations with
 * {@link CrankerConnectorBuilder#addInstrumentationListener(String, ConnectorInstrumentationListener)}.</p>
 * <p>Implementations must be thread safe; different requests may invoke a listener concurrently.
 * Context and cleanup state should belong to each invocation, not to the listener instance.</p>
 */
public interface ConnectorInstrumentationListener {
    /**
     * Inspects the prepared request as it's received from the router, before proxy listeners run.
     * <p>The request contains the headers prepared from the incoming tunnel request, after
     * connector header filtering but before application modifications. This lets instrumentation
     * establish incoming context for both the proxy callback and HTTP-client submission; it does
     * not observe the final headers returned by the proxy callback.</p>
     * <p>Callbacks run in registration order. Their cleanup runs in reverse order on the same
     * thread after synchronous submission to {@code HttpClient.sendAsync} and installation of
     * essential asynchronous response handling, including when another callback, the proxy listener,
     * sending or cleanup fails.</p>
     * <p>Instrumentation is best-effort. Recoverable failures from a callback or its cleanup are
     * logged with {@link java.util.logging}, without stopping later listeners, remaining cleanup,
     * or request forwarding. Returning null is treated as a callback failure.</p>
     * <p>A callback that throws before returning must clean up its own partial work. Cleanup actions
     * must restore their context even when they fail; the connector cannot repair listener-owned state.
     * Failures from the ordinary proxy callback or HTTP client still fail the request normally.</p>
     * <p>This lifetime does not confirm header delivery and does not cover request-body streaming,
     * response completion or arbitrary asynchronous callbacks. Do not consume the body publisher.</p>
     * <p>The default implementation does nothing and returns a no-op cleanup.</p>
     *
     * @param info Read-only information about the prepared request, before proxy-listener modifications
     * @return Non-null per-invocation cleanup; a no-op runnable is allowed
     */
    default Runnable beforeTargetDispatch(TargetDispatchInfo info) {
        return () -> {};
    }
}
