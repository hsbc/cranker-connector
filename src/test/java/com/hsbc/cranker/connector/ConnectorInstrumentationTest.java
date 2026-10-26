package com.hsbc.cranker.connector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ConnectorInstrumentationTest {
    @Test
    void defaultCallbackDoesNothingAndDoesNotPreventOtherListeners() {
        ConnectorInstrumentationListener optional = new ConnectorInstrumentationListener() {};
        Runnable cleanup = optional.beforeTargetDispatch(new TargetDispatchInfo(request()));
        assertNotNull(cleanup);
        assertDoesNotThrow(cleanup::run);

        List<String> calls = new ArrayList<>();
        ConnectorInstrumentation instrumentation = new ConnectorInstrumentation(List.of(
            optional, recording("active", calls), optional));
        Runnable[] cleanups = instrumentation.open(request());
        assertEquals(List.of("active"), calls);
        ConnectorInstrumentation.close(cleanups);
        assertEquals(List.of("active", "close active"), calls);
    }

    @Test
    void instrumentationInspectsOneImmutableInputWithoutConsumingItsBody() {
        HttpRequest.BodyPublisher body = new HttpRequest.BodyPublisher() {
            @Override public long contentLength() { return 1; }
            @Override public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
                fail("instrumentation must not consume the body");
            }
        };
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost/"))
            .POST(body).header("x-repeated", "one").header("X-Repeated", "two").build();
        List<TargetDispatchInfo> inputs = new ArrayList<>();
        ConnectorInstrumentationListener listener = new ConnectorInstrumentationListener() {
            @Override
            public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                inputs.add(info);
                assertSame(request, info.request());
                assertSame(body, info.request().bodyPublisher().orElseThrow());
                assertEquals(List.of("one", "two"), info.request().headers().allValues("x-repeated"));
                return () -> {};
            }
        };
        ConnectorInstrumentation instrumentation = new ConnectorInstrumentation(List.of(listener, listener));
        try (InstrumentationTestLog log = new InstrumentationTestLog()) {
            Runnable[] cleanup = instrumentation.open(request);
            ConnectorInstrumentation.close(cleanup);
            assertSame(inputs.get(0), inputs.get(1));
            assertTrue(log.failures.isEmpty(), log.failures.toString());
        }
        assertThrows(NullPointerException.class, () -> new TargetDispatchInfo(null));
    }

    static Stream<Throwable> recoverableFailures() {
        return Stream.of(new IllegalStateException("runtime"), new AssertionError("assertion"),
            new LinkageError("linkage"), new Exception("undeclared"));
    }

    @ParameterizedTest
    @MethodSource("recoverableFailures")
    void failedOpenerDoesNotStopLaterCallbacksOrTheirReverseCleanup(Throwable failure) {
        List<String> calls = new ArrayList<>();
        ConnectorInstrumentation instrumentation = new ConnectorInstrumentation(List.of(
            recording("first", calls),
            new ConnectorInstrumentationListener() {
                @Override
                public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                    calls.add("failed");
                    ConnectorInstrumentationTest.<RuntimeException>throwUnchecked(failure);
                    return null;
                }
            },
            recording("last", calls)
        ));
        try (InstrumentationTestLog log = new InstrumentationTestLog()) {
            Runnable[] cleanup = instrumentation.open(request());
            assertEquals(List.of("first", "failed", "last"), calls);
            ConnectorInstrumentation.close(cleanup);
            assertEquals(List.of("first", "failed", "last", "close last", "close first"), calls);
            assertEquals(List.of(failure), log.failures);
        }
    }

    @Test
    void nullCleanupIsReportedWithoutStoppingLaterCallbacks() {
        List<String> calls = new ArrayList<>();
        ConnectorInstrumentation instrumentation = new ConnectorInstrumentation(List.of(
            recording("first", calls), new ConnectorInstrumentationListener() {
                @Override
                public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                    return null;
                }
            }, recording("last", calls)));
        try (InstrumentationTestLog log = new InstrumentationTestLog()) {
            Runnable[] cleanup = instrumentation.open(request());
            assertEquals(List.of("first", "last"), calls);
            ConnectorInstrumentation.close(cleanup);
            assertEquals(List.of("first", "last", "close last", "close first"), calls);
            assertEquals(1, log.failures.size());
            assertInstanceOf(NullPointerException.class, log.failures.get(0));
        }
    }

    @ParameterizedTest
    @MethodSource("recoverableFailures")
    void cleanupFailuresAreReportedWithoutStoppingOtherCleanup(Throwable first) {
        Error second = new AssertionError("second cleanup failure");
        List<String> calls = new ArrayList<>();
        ConnectorInstrumentation instrumentation = new ConnectorInstrumentation(List.of(
            recording("first", calls),
            new ConnectorInstrumentationListener() {
                @Override
                public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                    return () -> { throw second; };
                }
            },
            new ConnectorInstrumentationListener() {
                @Override
                public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                    return () -> ConnectorInstrumentationTest.<RuntimeException>throwUnchecked(first);
                }
            }
        ));
        try (InstrumentationTestLog log = new InstrumentationTestLog()) {
            Runnable[] cleanup = instrumentation.open(request());
            ConnectorInstrumentation.close(cleanup);
            assertEquals(List.of(first, second), log.failures);
            assertEquals(List.of("first", "close first"), calls);
            assertEquals(0, first.getSuppressed().length);
        }
    }

    static Stream<Error> fatalFailures() {
        return Stream.of(new InternalError("fatal"), new StackOverflowError("fatal"));
    }

    @ParameterizedTest
    @MethodSource("fatalFailures")
    void fatalOpenFailureUnwindsEarlierListenersAndEscapes(Error fatal) {
        List<String> calls = new ArrayList<>();
        ConnectorInstrumentation instrumentation = new ConnectorInstrumentation(List.of(
            recording("first", calls), new ConnectorInstrumentationListener() {
                @Override
                public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                    throw fatal;
                }
            }, recording("never", calls)));
        try (InstrumentationTestLog log = new InstrumentationTestLog()) {
            assertSame(fatal, assertThrows(Error.class, () -> instrumentation.open(request())));
            assertEquals(List.of("first", "close first"), calls);
            assertTrue(log.failures.isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fatalOpenFailureRemainsPrimaryWhenCleanupAlsoFails(boolean sameFailure) {
        Error fatal = new InternalError("open");
        Error cleanupFailure = sameFailure ? fatal : new InternalError("cleanup");
        ConnectorInstrumentation instrumentation = new ConnectorInstrumentation(List.of(
            new ConnectorInstrumentationListener() {
                @Override
                public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                    return () -> { throw cleanupFailure; };
                }
            },
            new ConnectorInstrumentationListener() {
                @Override
                public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                    throw fatal;
                }
            }
        ));
        try (InstrumentationTestLog log = new InstrumentationTestLog()) {
            assertSame(fatal, assertThrows(Error.class, () -> instrumentation.open(request())));
            assertArrayEquals(sameFailure ? new Throwable[0] : new Throwable[] {cleanupFailure},
                fatal.getSuppressed());
            assertTrue(log.failures.isEmpty());
        }
    }

    @ParameterizedTest
    @MethodSource("fatalFailures")
    void fatalCleanupFailureEscapes(Error fatal) {
        try (InstrumentationTestLog log = new InstrumentationTestLog()) {
            assertSame(fatal, assertThrows(Error.class,
                () -> ConnectorInstrumentation.close(new Runnable[] {() -> { throw fatal; }})));
            assertTrue(log.failures.isEmpty());
        }
    }

    private static HttpRequest request() {
        return HttpRequest.newBuilder(URI.create("http://localhost/original")).build();
    }

    private static ConnectorInstrumentationListener recording(String name, List<String> calls) {
        return new ConnectorInstrumentationListener() {
            @Override
            public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                calls.add(name);
                return () -> calls.add("close " + name);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
        throw (T) failure;
    }
}
