package com.hsbc.cranker.connector;


import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CrankerConnectorStopTest extends BaseEndToEndTest {


    @Test
    public void throwIllegalStateExceptionWhenCallingStopBeforeCallingStart() {

        CrankerConnector connector = CrankerConnectorBuilder.connector()
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withRouterUris(RegistrationUriSuppliers.fixedUris(registrationUri()))
            .withRoute("*")
            .withTarget(URI.create("https://test-url"))
            .withComponentName("cranker-connector-unit-test")
            .build(); // not start


        // call before start
        Exception exception = assertThrows(IllegalStateException.class, connector::stop);

        assertExceptionMessage(exception);
    }


    @Test
    public void throwIllegalStateExceptionWhenCallingStopMultipleTime() {
        CrankerConnector connector = CrankerConnectorBuilder.connector()
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withRouterUris(RegistrationUriSuppliers.fixedUris(registrationUri()))
            .withRoute("*")
            .withTarget(URI.create("https://test-url"))
            .withComponentName("cranker-connector-unit-test")
            .start();

        // call stop the first time
        connector.stop();

        // second time will throw exception
        Exception exception = assertThrows(IllegalStateException.class, connector::stop);

        assertExceptionMessage(exception);
    }

    private void assertExceptionMessage(Exception exception) {
        String expectedMessage = "Cannot call stop() when the connector is not running. Did you call stop() twice?";
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);
    }
}