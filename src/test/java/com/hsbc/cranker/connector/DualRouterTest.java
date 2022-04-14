package com.hsbc.cranker.connector;

import io.muserver.MuHandler;
import io.muserver.MuServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import scaffolding.StringUtils;
import scaffolding.testrouter.CrankerRouter;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import static io.muserver.ContextHandlerBuilder.context;
import static io.muserver.MuServerBuilder.httpServer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static scaffolding.Action.swallowException;
import static scaffolding.testrouter.CrankerRouterBuilder.crankerRouter;

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

    private static final MuServer targetA1 = httpServer().addHandler(context("a").addHandler(textHandler)).start();
    private static final MuServer targetA2 = httpServer().addHandler(context("a").addHandler(textHandler)).start();
    private static final MuServer targetB = httpServer().addHandler(context("b").addHandler(textHandler)).start();
    private static final MuServer targetCatchAll = httpServer().addHandler(textHandler).start();
    private static final CrankerRouter router1 = crankerRouter().start();
    private static final MuServer routerServer1 = httpServer().addHandler(router1.createRegistrationHandler()).addHandler(router1.createHttpHandler()).start();
    private static final CrankerRouter router2 = crankerRouter().start();
    private static final MuServer routerServer2 = httpServer().addHandler(router2.createRegistrationHandler()).addHandler(router2.createHttpHandler()).start();
    private static final CrankerConnector connector1 = BaseEndToEndTest.startConnectorAndWaitForRegistration(router1, "a", targetA1, 2, routerServer1, routerServer2);
    private static final CrankerConnector connector2 = BaseEndToEndTest.startConnectorAndWaitForRegistration(router1, "b", targetB, 2, routerServer1, routerServer2);
    private static final CrankerConnector connector3 = BaseEndToEndTest.startConnectorAndWaitForRegistration(router1, "a", targetA2, 2, routerServer1, routerServer2);
    private static final CrankerConnector connector4 = BaseEndToEndTest.startConnectorAndWaitForRegistration(router1, "*", targetCatchAll, 2, routerServer1, routerServer2);

    @Test
    public void canMakeGETRequestsToBothConnectorApp() throws Exception {
        assertGetLargeHtmlWorks(routerServer1, "/a");
        assertGetLargeHtmlWorks(routerServer1, "/b");
        assertGetLargeHtmlWorks(routerServer1, "");
        assertGetLargeHtmlWorks(routerServer2, "/a");
        assertGetLargeHtmlWorks(routerServer2, "/b");
        assertGetLargeHtmlWorks(routerServer2, "");
    }

    @Test
    public void canMakeRequestWhenOneConnectorAppIsDown() throws Exception {
        connector1.stop().get(1, TimeUnit.MINUTES);
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

    @AfterAll
    public static void stop() {
        CrankerConnector[] connectors = {connector1, connector2, connector3, connector4};
        for (CrankerConnector connector : connectors) {
            swallowException(() -> connector.stop().get(30, TimeUnit.SECONDS));
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

}
