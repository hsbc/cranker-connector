package manual;

import com.hsbc.cranker.connector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hsbc.cranker.connector.CrankerConnectorBuilder.CRANKER_PROTOCOL_3;

public class RunLocalConnector {

    private static final Logger log = LoggerFactory.getLogger(RunLocalConnector.class);

    static {
        System.setProperty("jdk.internal.httpclient.disableHostnameVerification", "true");
    }

    public static void main(String[] args) {

        final CrankerConnector connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(List.of(CRANKER_PROTOCOL_3))
            .withDomain("127.0.0.1")
            .withRouterUris(() -> List.of(URI.create("wss://localhost:12666")))
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withComponentName("local-test")
            .withRoute("*")
            .withTarget(URI.create("http://localhost:14444"))
            .withRouterRegistrationListener(new RouterEventListener() {
                public void onRegistrationChanged(ChangeData data) {
                    log.info("Router registration changed: " + data);
                }
                public void onSocketConnectionError(RouterRegistration router1, Throwable exception) {
                    log.warn("Error connecting to " + router1, exception);
                }
            })
            .withProxyEventListener(new ProxyEventListener() {
                @Override
                public void onProxyError(HttpRequest request, Throwable error) {
                    log.warn("onProxyError, request="+ request, error);
                }
            })
            .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            final boolean stop = connector.stop(5, TimeUnit.SECONDS);
            log.info("connector stop, state={}", stop);
        }));

        System.out.println("connector started");
    }
}
