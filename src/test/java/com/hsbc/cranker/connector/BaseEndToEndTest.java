package com.hsbc.cranker.connector;

import com.hsbc.cranker.mucranker.CrankerRouter;
import com.hsbc.cranker.mucranker.CrankerRouterBuilder;
import io.muserver.MuServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepetitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scaffolding.AssertUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.hsbc.cranker.connector.CrankerConnectorBuilder.CRANKER_PROTOCOL_1;
import static com.hsbc.cranker.connector.CrankerConnectorBuilder.CRANKER_PROTOCOL_3;
import static io.muserver.MuServerBuilder.muServer;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static scaffolding.AssertUtils.assertEventually;

public class BaseEndToEndTest {

    protected final HttpClient testClient = HttpUtils.createHttpClientBuilder(true).build();

    protected CrankerRouter crankerRouter = CrankerRouterBuilder
        .crankerRouter()
        .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
        .start();
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
        return startConnectorAndWaitForRegistration(crankerRouter, targetServiceName, target, List.of(CRANKER_PROTOCOL_1), slidingWindowSize, registrationRouters);
    }

    public static CrankerConnector startConnectorAndWaitForRegistration(CrankerRouter crankerRouter,
                                                                        String targetServiceName,
                                                                        MuServer target,
                                                                        List<String> preferredProtocols,
                                                                        int slidingWindowSize,
                                                                        MuServer... registrationRouters) {
        CrankerConnector connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols)
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withTarget(target.uri())
            .withRoute(targetServiceName)
            .withRouterUris(RegistrationUriSuppliers.fixedUris(Stream.of(registrationRouters)
                .map(s -> registrationUri(s.uri()))
                .collect(toList())))
            .withSlidingWindowSize(slidingWindowSize)
            .start();

        waitForRegistration(targetServiceName, connector.connectorId(), 2, crankerRouter);

        assertEventually(
            () -> new ArrayList<>(connector.routers().get(0).idleSockets()).get(0).version(),
            equalTo(preferredProtocols.get(0)));

        return connector;
    }

    public static void waitForRegistration(String targetServiceName, String connectorInstanceId, int slidingWindow, CrankerRouter... crankerRouters) {
        final String serviceKey = targetServiceName.isEmpty() ? "*" : targetServiceName;
        for (CrankerRouter crankerRouter : crankerRouters) {
            AssertUtils.assertEventually(() -> crankerRouter.collectInfo().services()
                .stream()
                .anyMatch(service -> service.route().equals(serviceKey)
                    && !service.connectors().isEmpty()
                    && service.connectors()
                    .stream()
                    .anyMatch(connector -> connector.connectorInstanceID().equals(connectorInstanceId)
                        && connector.connections().size() >= slidingWindow)
                ), is(true));
        }
    }

    public static List<String> preferredProtocols(RepetitionInfo repetitionInfo) {
        final int currentRepetition = repetitionInfo.getCurrentRepetition();
        switch (currentRepetition) {
            case 1:
                return List.of(CRANKER_PROTOCOL_1);
            case 2:
                return List.of(CRANKER_PROTOCOL_3);
            default:
                return List.of(CRANKER_PROTOCOL_3, CRANKER_PROTOCOL_1);
        }
    }

    @AfterEach
    public void stopServers() {
        crankerServer.stop();
        registrationServer.stop();
        crankerRouter.stop();
    }

}
