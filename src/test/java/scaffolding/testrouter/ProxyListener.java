package scaffolding.testrouter;

import io.muserver.Headers;

import javax.ws.rs.WebApplicationException;
import java.util.List;

/**
 * Hooks to intercept, change and observe the proxying of requests from a client to a target and back.
 * <p>Register listeners when constructor the router with the {@link CrankerRouterBuilder#withProxyListeners(List)}
 * method.</p>
 * <p><strong>Important note:</strong> It is recommended to extend {@link NoOpProxyListener}
 * rather than implement this interface so that extra methods can be added to this interface
 * without causing compilation errors.</p>
 */
public interface ProxyListener {

    /**
     * This is called before sending a request to the target service
     *
     * @param info                   Info about the request and response. Note that duration, bytes sent and bytes received
     *                               will contain values as the the current point in time.
     * @param requestHeadersToTarget The headers that will be sent to the client. Modify this object in order to change
     *                               the headers sent to the target server.
     * @throws WebApplicationException Throw a web application exception to send an error to the client rather than
     *                                 proxying the request.
     */
    void onBeforeProxyToTarget(ProxyInfo info, Headers requestHeadersToTarget) throws WebApplicationException;

    /**
     * <p>This is called before sending the response to the client.</p>
     * <p>The response info contains the response objects with the HTTP status and headers that will be sent
     * to the client. You are able to change these values at this point in the lifecycle.</p>
     *
     * @param info Info about the request and response. Note that duration, bytes sent and bytes received
     *             will contain values as the the current point in time.
     * @throws WebApplicationException Throw a web application exception to send an error to the client rather than
     *                                 proxying the response. This allows you reject responses from services that
     *                                 you deem to be invalid.
     */
    void onBeforeRespondingToClient(ProxyInfo info) throws WebApplicationException;

    /**
     * This is called after a response has been completed.
     * <p>Note that this is called even if the response was not completed successfully (for example if a browser
     * was closed before a response was complete). If the proxying was not successful, then
     * {@link ProxyInfo#errorIfAny()} will not be null.</p>
     *
     * @param proxyInfo Information about the response.
     */
    void onComplete(ProxyInfo proxyInfo);

    /**
     * This is called if a free socket could not be found for the target.  The socket is used to proxy the request to the target service.
     *
     * {@link ProxyInfo#bytesReceived()} will be 0.</p>
     * {@link ProxyInfo#bytesSent()} will be 0.</p>
     * {@link ProxyInfo#connectorInstanceID()} will be null.</p>
     * {@link ProxyInfo#serviceAddress()} will be null.</p>
     * {@link ProxyInfo#errorIfAny()} will be null.</p>
     *
     * @param proxyInfo Information about the request.
     */
    void onFailureToAcquireProxySocket(ProxyInfo proxyInfo);
}
