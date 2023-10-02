package com.hsbc.cranker.connector;

import com.hsbc.cranker.mucranker.CrankerRouter;
import io.muserver.Method;
import io.muserver.MuServer;
import io.muserver.SsePublisher;
import org.junit.jupiter.api.*;
import scaffolding.SseTestClient;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;
import static io.muserver.ContextHandlerBuilder.context;
import static io.muserver.MuServerBuilder.httpServer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static scaffolding.Action.swallowException;
import static scaffolding.AssertUtils.assertEventually;

public class RouterSupplierTest extends BaseEndToEndTest {

    private static final String route = "my-service";

    private MuServer target;
    private CrankerRouter router1;
    private MuServer routerServer1;
    private CrankerRouter router2;
    private MuServer routerServer2;

    @RepeatedTest(3)
    public void dnsLookupCanWorkForLocalhost(RepetitionInfo repetitionInfo) throws Exception {
        CrankerConnector connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols(repetitionInfo))
            .withTarget(target.uri())
            .withRoute(route)
            .withRouterLookupByDNS(registrationUri(routerServer1), registrationUri(routerServer2))
            .start();
        waitForRegistration(route, connector.connectorId(), 2, router1);
        waitForRegistration(route, connector.connectorId(), 2, router2);
        for (URI routerUri : List.of(routerServer1.uri(), routerServer2.uri())) {
            for (int i = 0; i < 4; i++) { // when resolving localhost, it may get two URLs (an IP4 and IP6), so just do some extra calls to make sure all are hit at least once
                assertGetWorks(routerUri);
            }
        }
        assertStoppable(connector);
    }

    @RepeatedTest(3)
    public void routersCanBeDynamicallyAddedAndRemoved(RepetitionInfo repetitionInfo) throws Exception {

        final AtomicReference<RouterEventListener.ChangeData> changeData = new AtomicReference<>();
        URI reg1 = registrationUri(routerServer1);
        URI reg2 = registrationUri(routerServer2);
        List<URI> routerUris = new ArrayList<>(List.of(reg1, reg2));
        final List<String> preferredProtocols = preferredProtocols(repetitionInfo);
        final int expectErrorCode = preferredProtocols.get(0).startsWith("cranker_1.0") ? 503 : 404;
        CrankerConnectorImpl connector = (CrankerConnectorImpl) CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols)
            .withSlidingWindowSize(2)
            .withTarget(target.uri())
            .withRoute(route)
            .withRouterUris(() -> routerUris)
            .withRouterRegistrationListener(new RouterEventListener() {
                @Override
                public void onRegistrationChanged(ChangeData data) {
                    changeData.set(data);
                }
            })
            .start();

        waitForRegistration(route, connector.connectorId(), 2, router1);
        waitForRegistration(route, connector.connectorId(), 2, router2);

        assertThat(changeData.get().added(), hasSize(2));
        assertThat(changeData.get().removed(), hasSize(0));
        assertThat(changeData.get().unchanged(), hasSize(0));

        assertGetWorks(routerServer1.uri());
        assertGetWorks(routerServer2.uri());

        final List<RouterRegistration> registrations = connector.routers();
        assertThat(registrations.size(), is(2));
        assertThat(registrations.get(0).idleSocketSize(), is(2));
        assertThat(registrations.get(1).idleSocketSize(), is(2));

        routerUris.remove(reg2);
        connector.updateRoutersAsync().get();

        assertThat(changeData.get().added(), hasSize(0));
        assertThat(changeData.get().removed(), hasSize(1));
        assertThat(changeData.get().removed().get(0).registrationUri().resolve("/"), equalTo(reg2.resolve("/")));
        assertThat(changeData.get().unchanged(), hasSize(1));
        assertThat(changeData.get().unchanged().get(0).registrationUri().resolve("/"), equalTo(reg1.resolve("/")));

        assertGetWorks(routerServer1.uri());
        assertGetNotWorks(routerServer2.uri(), expectErrorCode);

        final List<RouterRegistration> registrationsAfterRemove2 = connector.routers();
        assertThat(registrationsAfterRemove2.size(), is(1));
        assertThat(registrationsAfterRemove2.get(0).idleSocketSize(), is(2));

        routerUris.remove(reg1);
        routerUris.add(reg2);
        connector.updateRoutersAsync().get();

        assertThat(changeData.get().added(), hasSize(1));
        assertThat(changeData.get().added().get(0).registrationUri().resolve("/"), equalTo(reg2.resolve("/")));
        assertThat(changeData.get().removed(), hasSize(1));
        assertThat(changeData.get().removed().get(0).registrationUri().resolve("/"), equalTo(reg1.resolve("/")));
        assertThat(changeData.get().unchanged(), hasSize(0));

        assertGetNotWorks(routerServer1.uri(), expectErrorCode);
        waitForRegistration(route, connector.connectorId(), 2, router2);
        assertGetWorks(routerServer2.uri());

        assertStoppable(connector);
    }

    @RepeatedTest(3)
    public void routersCanBeUpdatedWhenUpdateRouteTaskTimeout(RepetitionInfo repetitionInfo) throws Exception {
        final AtomicReference<RouterEventListener.ChangeData> changeData = new AtomicReference<>();
        URI reg1 = registrationUri(routerServer1);
        URI reg2 = registrationUri(routerServer2);
        List<URI> routerUris = new ArrayList<>(List.of(reg1, reg2));
        CrankerConnectorImpl connector = (CrankerConnectorImpl) CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols(repetitionInfo))
            .withTarget(target.uri())
            .withRoute(route)
            .withRouterUris(() -> routerUris)
            .withRouterRegistrationListener(new RouterEventListener() {
                @Override
                public void onRegistrationChanged(ChangeData data) {
                    changeData.set(data);
                }
            })
            .withRouterUpdateInterval(500, TimeUnit.MILLISECONDS)
            .start();

        waitForRegistration(route, connector.connectorId(),1, router1);
        waitForRegistration(route, connector.connectorId(), 1, router2);

        assertThat(changeData.get().added(), hasSize(2));
        assertThat(changeData.get().removed(), hasSize(0));
        assertThat(changeData.get().unchanged(), hasSize(0));

        SseTestClient client = SseTestClient.startSse(routerServer2.uri().resolve("/my-service/sse/counter"));
        routerUris.remove(reg2);
        Thread.sleep(1000);

        assertThat(changeData.get().added(), hasSize(0));
        assertThat(changeData.get().removed(), hasSize(1));
        assertThat(changeData.get().unchanged(), hasSize(1));

        // clean it
        changeData.set(new RouterEventListener.ChangeData(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
        routerUris.remove(reg1);
        routerUris.add(reg2);

        // reach timeout 500ms
        Thread.sleep(1000);

        // routers can be updated
        assertThat(changeData.get().added(), hasSize(1));
        assertThat(changeData.get().removed(), hasSize(1));
        assertThat(changeData.get().unchanged(), hasSize(0));

        assertStoppable(connector);
        client.stop();
    }

    @RepeatedTest(3)
    @Disabled("Takes a minute to run so not normally run as part of build")
    public void nonExistentDomainsReturnErrorsToListener(RepetitionInfo repetitionInfo) throws Exception {
        AtomicReference<Throwable> received = new AtomicReference<>();
        AtomicBoolean useNonExistantDomain = new AtomicBoolean(false);
        var connector = (CrankerConnectorImpl) CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols(repetitionInfo))
            .withTarget(target.uri())
            .withRoute(route)
            .withRouterUris(() -> {
                if (!useNonExistantDomain.get()) {
                    // can't have an exception on the first lookup because that will get bubbled from .start()
                    return Set.of(URI.create("ws://localhost:9000"));
                } else {
                    return RegistrationUriSuppliers.dnsLookup(URI.create("ws://" + UUID.randomUUID() + ".example.hsbc:9000")).get();
                }
            })
            .withRouterRegistrationListener(new RouterEventListener() {
                @Override
                public void onRouterDnsLookupError(Throwable error) {
                    received.set(error);
                }
            })
            .start();

        useNonExistantDomain.set(true);
        Thread.sleep(45000);

        assertEventually(received::get, instanceOf(RuntimeException.class));
        connector.stop(1, TimeUnit.MINUTES);
    }


    private void assertGetWorks(URI routerUri) throws java.io.IOException, InterruptedException {
        HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
            .uri(routerUri.resolve("/" + route + "/hello"))
            .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertEquals("Hello from target", resp.body());
    }

    private void assertGetNotWorks(URI routerUri, int expectedStatusCode) {
        assertEventually(() -> {
            HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
                .uri(routerUri.resolve("/" + route + "/hello"))
                .build(), HttpResponse.BodyHandlers.ofString());
            return resp.statusCode();
        }, equalTo(expectedStatusCode));
    }


    private void assertStoppable(CrankerConnector connector) {
        Assertions.assertDoesNotThrow(() -> connector.stop(30, TimeUnit.SECONDS));
    }

    @BeforeEach
    public void start() {
        this.target = httpServer()
            .addHandler(context(route).addHandler(Method.GET, "/hello", (request, response, pathParams) -> response.write("Hello from target")))
            .addHandler(Method.GET, "/my-service/sse/counter", (request, response, pathParams) -> {
                SsePublisher publisher = SsePublisher.start(request, response);
                for (int i1 = 0; i1 < 1000; i1++) {
                    try {
                        publisher.send("Number " + i1);
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        // The user has probably disconnected so stopping
                        break;
                    }
                }
                publisher.close();
            })
            .start();
        this.router1 = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();
        this.routerServer1 = httpServer().addHandler(router1.createRegistrationHandler()).addHandler(router1.createHttpHandler()).start();
        this.router2 = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();
        this.routerServer2 = httpServer().addHandler(router2.createRegistrationHandler()).addHandler(router2.createHttpHandler()).start();
    }

    @AfterEach
    public void stop() {
        swallowException(routerServer1::stop);
        swallowException(routerServer2::stop);
        swallowException(target::stop);
        swallowException(router1::stop);
        swallowException(router2::stop);
    }


    private static URI registrationUri(MuServer routerServer) {
        String scheme = routerServer.uri().getScheme().equals("http") ? "ws" : "wss";
        return URI.create(scheme + "://" + routerServer.uri().getAuthority());
    }
}
