package scaffolding.testrouter;

import io.muserver.MuRequest;
import io.muserver.MuResponse;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Information about a proxied request and response. Use {@link CrankerRouterBuilder#withProxyListeners(List)} to subscribe
 * to events that exposes this data.
 */
public interface ProxyInfo {

    /**
     * @return Returns true if the connector is a catch-all connector (i.e. the router of the connector is '*').
     */
    boolean isCatchAll();

    /**
     * @return A unique ID for the service connector.
     */
    String connectorInstanceID();

    /**
     * @return The address of the service connector that this request is being proxied to.
     */
    InetSocketAddress serviceAddress();

    /**
     * @return The cranker route (i.e. the first part of a path) for the request, or '*' if a catch-all connector is used.
     */
    String route();

    /**
     * @return The client's request to the router.
     */
    MuRequest request();

    /**
     * @return The router's response to the client.
     */
    MuResponse response();

    /**
     * @return The time in millis from when the router received the request until it sent the last response byte.
     */
    long durationMillis();

    /**
     * @return The number of bytes uploaded by the client in the request
     */
    long bytesReceived();

    /**
     * @return The number of bytes sent to the client on the response
     */
    long bytesSent();

    /**
     * If the response was not proxied successfully, then this has the exception.
     * @return null if no problems, otherwise an exception
     */
    Throwable errorIfAny();

    /**
     * @return number of attempts to get a websocket (which is used for proxy requests)
     */
    int socketAttempts();
}
