package scaffolding.testrouter;

import io.muserver.Headers;

import javax.ws.rs.WebApplicationException;

/**
 * <p>A No-Op implementation of the proxy listener.</p>
 * <p>It is recommended that your own custom listeners extend this base class rather than implementing
 * the {@link ProxyListener} directly so that when new event methods are added your implementation
 * class will not break.</p>
 */
public class NoOpProxyListener implements ProxyListener {
    @Override
    public void onBeforeProxyToTarget(ProxyInfo info, Headers requestHeadersToTarget) throws WebApplicationException {
    }

    @Override
    public void onBeforeRespondingToClient(ProxyInfo info) {
    }

    @Override
    public void onComplete(ProxyInfo proxyInfo) {
    }

    @Override
    public void onFailureToAcquireProxySocket(ProxyInfo proxyInfo) {
    }
}
