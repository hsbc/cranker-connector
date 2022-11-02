package com.hsbc.cranker.connector;

import io.muserver.MuServer;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scaffolding.testrouter.CrankerRouter;
import scaffolding.testrouter.CrankerRouterBuilder;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.List;
import java.util.stream.Stream;

import static io.muserver.MuServerBuilder.muServer;
import static java.util.stream.Collectors.toList;

public class BaseEndToEndTest {

    private static final Logger log = LoggerFactory.getLogger(BaseEndToEndTest.class);

    protected final HttpClient testClient = HttpUtils.createHttpClientBuilder(true).build();

    protected CrankerRouter crankerRouter = CrankerRouterBuilder.crankerRouter().start();
    protected MuServer registrationServer = startRegistrationServer(0);
    protected MuServer crankerServer = startCrankerServer(0);

    protected MuServer startCrankerServer(int port) {
        return muServer().withHttpsPort(port).addHandler(crankerRouter.createHttpHandler()).start();
    }

    protected MuServer startRegistrationServer(int port) {
        return muServer().withHttpsPort(port).addHandler(crankerRouter.createRegistrationHandler()).start();
    }

    protected static URI registrationUri(URI routerUri) {
        return URI.create("ws" + routerUri.toString().substring(4));
    }

    public static CrankerConnector startConnectorAndWaitForRegistration(CrankerRouter crankerRouter,
                                                                 String targetServiceName,
                                                                 MuServer target,
                                                                 int slidingWindowSize,
                                                                 MuServer... registrationRouters) {
        CrankerConnector connector = startConnector(targetServiceName, target, slidingWindowSize, registrationRouters);
        waitForRegistration(targetServiceName, 2, crankerRouter);
        return connector;
    }

    public static void waitForRegistration(String targetServiceName, int slidingWindow, CrankerRouter... crankerRouters) {
        int attempts = 0;
        final String serviceKey = targetServiceName.isEmpty() ? "*" : targetServiceName;
        for (CrankerRouter crankerRouter : crankerRouters) {
            while (crankerRouter.collectInfo().services()
                    .stream()
                    .noneMatch(service -> service.route().equals(serviceKey)
                        && service.connectors().size() > 0
                        && service.connectors().get(0).connections().size() >= slidingWindow)) {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (attempts++ == 100) throw new RuntimeException("Failed to register " + targetServiceName);
            }
        }
    }

    public static CrankerConnector startConnector(String targetServiceName, MuServer target, int slidingWindowSize, MuServer... registrationRouters) {
        List<URI> uris = Stream.of(registrationRouters)
            .map(s -> registrationUri(s.uri()))
            .collect(toList());
        return CrankerConnectorBuilder.connector()
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withTarget(target.uri())
            .withRoute(targetServiceName)
            .withRouterUris(RegistrationUriSuppliers.fixedUris(uris))
            .withSlidingWindowSize(slidingWindowSize)
            .start();
    }

    @AfterEach
    public void stopServers() {
        crankerServer.stop();
        registrationServer.stop();
        crankerRouter.stop();
    }

}
