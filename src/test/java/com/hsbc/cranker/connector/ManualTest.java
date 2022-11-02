package com.hsbc.cranker.connector;

import io.muserver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.muserver.MuServerBuilder.muServer;

public class ManualTest {
    private static final Logger log = LoggerFactory.getLogger(ManualTest.class);

    public static void main(String[] args) {

         System.getProperties().setProperty("jdk.internal.httpclient.disableHostnameVerification", Boolean.TRUE.toString());

        // start a cranker router and then run this to connect a dummy target service to it
        List<URI> routerRegistrationUris = List.of(
            URI.create("wss://localhost:9070")
        );

        MuServer targetServer = muServer()
            .withHttpsPort(12123)
            .withHttp2Config(Http2ConfigBuilder.http2Enabled())
            .addHandler(Method.GET, "/hi", (request, response, pathParams) -> {
                System.out.println("received /hi");
                response.write("hi");
            })
            .addHandler(Method.GET, "/health", (request, response, pathParams) -> {
                response.contentType("text/plain;charset=utf-8");
                for (HttpConnection con : request.server().activeConnections()) {
                    response.sendChunk(con.protocol() + " on " +  con.httpsProtocol() + " " + con.remoteAddress() + "\n");
                    for (MuRequest activeRequest : con.activeRequests()) {
                        response.sendChunk("   " + activeRequest + "\n");
                    }
                    response.sendChunk("\n");
                }
                response.sendChunk("-------");
            })
            .addHandler((request, response) -> {
                response.contentType(ContentTypes.TEXT_PLAIN_UTF8);
                response.sendChunk("Got " + request + "\n");
                response.sendChunk("Headers: " + request.headers() + "\n\n");
                response.sendChunk("Body:\n\n" + request.readBodyAsString());
                response.sendChunk("\n\nResponse ends");
                return true;
            })
            .start();

        AtomicInteger exceptionCounter = new AtomicInteger(0);
        CrankerConnector connector = CrankerConnectorBuilder.connector()
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withRouterUris(RegistrationUriSuppliers.fixedUris(routerRegistrationUris))
            .withRoute("*")
            .withTarget(targetServer.uri())
            .withComponentName("cranker-connector-manual-test")
            .withSlidingWindowSize(2)
            .withRouterRegistrationListener(new RouterEventListener() {
                @Override
                public void onSocketConnectionError(RouterRegistration router, Throwable exception) {
                    exceptionCounter.incrementAndGet();
                    log.warn("onSocketConnectionError, router={}, error={}", router, exception.getMessage());
                }
            })
            .start();
        log.info("Connector started with target " + targetServer.uri());

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(() -> {
            for (RouterRegistration router : connector.routers()) {
                log.info("Router expects {} and is {}, errorCount={}", router.expectedWindowSize(), router.idleSocketSize(), exceptionCounter.get());
//                for (ConnectorSocket idleSocket : router.idleSockets()) {
//                    log.info(" -- " + idleSocket);
//                }
            }
        }, 0, 5, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down....");
            executorService.shutdown();
            try {
                connector.stop(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.info("Error stopping connector", e);
            }
            targetServer.stop();
            log.info("Shutdown complete");
        }));

    }

}
