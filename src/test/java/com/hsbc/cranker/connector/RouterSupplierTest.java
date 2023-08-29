package com.hsbc.cranker.connector;

import io.muserver.Method;
import io.muserver.MuServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import com.hsbc.cranker.mucranker.CrankerRouter;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.muserver.ContextHandlerBuilder.context;
import static io.muserver.MuServerBuilder.httpServer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static scaffolding.Action.swallowException;
import static scaffolding.AssertUtils.assertEventually;
import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;

public class RouterSupplierTest extends BaseEndToEndTest {

    private static final String route = "my-service";

    private static final MuServer target = httpServer().addHandler(context(route).addHandler(Method.GET, "/hello", (request, response, pathParams) -> response.write("Hello from target"))).start();
    private static final CrankerRouter router1 = crankerRouter().start();
    private static final MuServer routerServer1 = httpServer().addHandler(router1.createRegistrationHandler()).addHandler(router1.createHttpHandler()).start();
    private static final CrankerRouter router2 = crankerRouter().start();
    private static final MuServer routerServer2 = httpServer().addHandler(router2.createRegistrationHandler()).addHandler(router2.createHttpHandler()).start();

    @Test
    public void dnsLookupCanWorkForLocalhost() throws Exception {
        CrankerConnector connector = CrankerConnectorBuilder.connector()
            .withTarget(target.uri())
            .withRoute(route)
            .withRouterLookupByDNS(registrationUri(routerServer1), registrationUri(routerServer2))
            .start();
        waitForRegistration(route, 2, router1);
        waitForRegistration(route, 2, router2);
        for (URI routerUri : List.of(routerServer1.uri(), routerServer2.uri())) {
            for (int i = 0; i < 4; i++) { // when resolving localhost, it may get two URLs (an IP4 and IP6), so just do some extra calls to make sure all are hit at least once
                assertGetWorks(routerUri);
            }
        }
        assertStoppable(connector);
    }

    RouterEventListener.ChangeData changeData;
    @Test
    public void routersCanBeDynamicallyAddedAndRemoved() throws Exception {

        URI reg1 = registrationUri(routerServer1);
        URI reg2 = registrationUri(routerServer2);
        List<URI> routerUris = new ArrayList<>(List.of(reg1, reg2));
        CrankerConnectorImpl connector = (CrankerConnectorImpl) CrankerConnectorBuilder.connector()
            .withTarget(target.uri())
            .withRoute(route)
            .withRouterUris(() -> routerUris)
            .withRouterRegistrationListener(new RouterEventListener() {
                @Override
                public void onRegistrationChanged(ChangeData data) {
                    changeData = data;
                }
            })
            .start();

        waitForRegistration(route, 2, router1);
        waitForRegistration(route, 2, router2);

        assertThat(changeData.added(), hasSize(2));
        assertThat(changeData.removed(), hasSize(0));
        assertThat(changeData.unchanged(), hasSize(0));

        routerUris.remove(reg2);
        connector.updateRoutersAsync().get();

        assertThat(changeData.added(), hasSize(0));
        assertThat(changeData.removed(), hasSize(1));
        assertThat(changeData.removed().get(0).registrationUri().resolve("/"), equalTo(reg2.resolve("/")));
        assertThat(changeData.unchanged(), hasSize(1));
        assertThat(changeData.unchanged().get(0).registrationUri().resolve("/"), equalTo(reg1.resolve("/")));

        assertGetWorks(routerServer1.uri());
        assertGetDoesNotWork(routerServer2.uri());

        routerUris.remove(reg1);
        routerUris.add(reg2);
        connector.updateRoutersAsync().get();

        assertThat(changeData.added(), hasSize(1));
        assertThat(changeData.added().get(0).registrationUri().resolve("/"), equalTo(reg2.resolve("/")));
        assertThat(changeData.removed(), hasSize(1));
        assertThat(changeData.removed().get(0).registrationUri().resolve("/"), equalTo(reg1.resolve("/")));
        assertThat(changeData.unchanged(), hasSize(0));

        assertGetDoesNotWork(routerServer1.uri());
        waitForRegistration(route, 2, router2);
        assertGetWorks(routerServer2.uri());

        assertStoppable(connector);
    }

    @Test
    @Disabled("Takes a minute to run so not normally run as part of build")
    public void nonExistantDomainsReturnErrorsToListener() throws Exception {
        AtomicReference<Throwable> received = new AtomicReference<>();
        AtomicBoolean useNonExistantDomain = new AtomicBoolean(false);
        var connector = (CrankerConnectorImpl) CrankerConnectorBuilder.connector()
            .withTarget(target.uri())
            .withRoute(route)
            .withRouterUris(() -> {
                if (!useNonExistantDomain.get()) {
                    // can't have an exception on the first lookup because that will get bubbled from .start()
                    return Set.of(URI.create("ws://localhost:9000"));
                } else {
                    return RegistrationUriSuppliers.dnsLookup(URI.create("ws://" + UUID.randomUUID() + ".example.org:9000")).get();
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

    private void assertGetDoesNotWork(URI routerUri) {
        assertEventually(() -> {
            HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
                .uri(routerUri.resolve("/" + route + "/hello"))
                .build(), HttpResponse.BodyHandlers.ofString());
            return resp.statusCode();
        }, equalTo(503));
    }


    private void assertStoppable(CrankerConnector connector) {
        Assertions.assertDoesNotThrow(() -> connector.stop(30, TimeUnit.SECONDS));
    }

    @AfterAll
    public static void stop() {
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
