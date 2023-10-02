package com.hsbc.cranker.connector;

import com.hsbc.cranker.mucranker.CrankerRouter;
import io.muserver.MuHandler;
import io.muserver.MuServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import scaffolding.StringUtils;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;
import static io.muserver.ContextHandlerBuilder.context;
import static io.muserver.MuServerBuilder.httpServer;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static scaffolding.Action.swallowException;
import static scaffolding.AssertUtils.assertEventually;



public class DualRouterTest extends BaseEndToEndTest {

    private static final String TEXT = StringUtils.randomStringOfLength(100000);
    private static final MuHandler textHandler = (request, response) -> {
        if (request.relativePath().equals("/blah.txt")) {
            response.write(TEXT);
            return true;
        } else {
            return false;
        }
    };

    private MuServer targetA1;
    private MuServer targetA2;
    private MuServer targetB;
    private MuServer targetCatchAll;
    private CrankerRouter router1;
    private MuServer routerServer1;
    private CrankerRouter router2;
    private MuServer routerServer2;
    private CrankerConnector connector1;
    private CrankerConnector connector2;
    private CrankerConnector connector3;
    private CrankerConnector connector4;

    private static CrankerConnector startConnector(String targetServiceName,
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

        assertEventually(
            () -> new ArrayList<>(connector.routers().get(0).idleSockets()).get(0).version(),
            equalTo(preferredProtocols.get(0)));

        return connector;
    }


    @BeforeEach
    void setUp(RepetitionInfo repetitionInfo) {
        final List<String> preferredProtocols = preferredProtocols(repetitionInfo);

        targetA1 = httpServer().addHandler(context("a").addHandler(textHandler)).start();
        targetA2 = httpServer().addHandler(context("a").addHandler(textHandler)).start();
        targetB = httpServer().addHandler(context("b").addHandler(textHandler)).start();
        targetCatchAll = httpServer().addHandler(textHandler).start();

        router1 = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();
        routerServer1 = httpServer()
            .addHandler(router1.createRegistrationHandler())
            .addHandler(router1.createHttpHandler()).start();

        router2 = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();
        routerServer2 = httpServer()
            .addHandler(router2.createRegistrationHandler())
            .addHandler(router2.createHttpHandler()).start();

        connector1 = startConnector("a", targetA1,preferredProtocols, 2, routerServer1, routerServer2);
        waitForRegistration("a", connector1.connectorId(), 2, router1, router2);

        connector2 = startConnector("b", targetB, preferredProtocols,2, routerServer1, routerServer2);
        waitForRegistration("b", connector2.connectorId(), 2, router1, router2);

        connector3 = startConnector("a", targetA2, preferredProtocols,2, routerServer1, routerServer2);
        waitForRegistration("a", connector3.connectorId(), 2, router1, router2);

        connector4 = startConnector("*", targetCatchAll, preferredProtocols,2, routerServer1, routerServer2);
        waitForRegistration("*", connector4.connectorId(), 2, router1, router2);

    }

    @AfterEach
    public void stop() {
        CrankerConnector[] connectors = {connector1, connector2, connector3, connector4};
        for (CrankerConnector connector : connectors) {
            swallowException(() -> connector.stop(30, TimeUnit.SECONDS));
        }
        swallowException(routerServer1::stop);
        swallowException(routerServer2::stop);
        swallowException(targetA1::stop);
        swallowException(targetA2::stop);
        swallowException(targetB::stop);
        swallowException(targetCatchAll::stop);
        swallowException(router1::stop);
        swallowException(router2::stop);
    }

    @RepeatedTest(3)
    public void canMakeGETRequestsToBothConnectorApp() throws Exception {
        assertGetLargeHtmlWorks(routerServer1, "/a");
        assertGetLargeHtmlWorks(routerServer1, "/b");
        assertGetLargeHtmlWorks(routerServer1, "");
        assertGetLargeHtmlWorks(routerServer2, "/a");
        assertGetLargeHtmlWorks(routerServer2, "/b");
        assertGetLargeHtmlWorks(routerServer2, "");
    }

    @RepeatedTest(3)
    public void canMakeRequestWhenOneConnectorAppIsDown() throws Exception {
        assertThat(connector1.stop(1, TimeUnit.MINUTES), is(true));
        assertGetLargeHtmlWorks(routerServer1, "/a");
        assertGetLargeHtmlWorks(routerServer1, "/b");
        assertGetLargeHtmlWorks(routerServer1, "");
        assertGetLargeHtmlWorks(routerServer2, "/a");
        assertGetLargeHtmlWorks(routerServer2, "/b");
        assertGetLargeHtmlWorks(routerServer2, "");
        connector1.start();
        for (int i = 0; i < 3; i++) {
            assertGetLargeHtmlWorks(routerServer1, "/a");
            assertGetLargeHtmlWorks(routerServer2, "/a");
        }

    }

    private void assertGetLargeHtmlWorks(MuServer routerServer, String prefix) throws Exception {
        URI uri = routerServer.uri().resolve(prefix + "/blah.txt");
        HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
            .uri(uri).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode(), is(200));
        assertThat(resp.body(), is(TEXT));
    }

}
