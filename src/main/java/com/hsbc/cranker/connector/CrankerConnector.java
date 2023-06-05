package com.hsbc.cranker.connector;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hsbc.cranker.connector.HttpUtils.urlEncode;
import static java.util.stream.Collectors.toList;

/**
 * A cranker connector maintains connections to one of more routers.
 * <p>Create a connector builder with {@link CrankerConnectorBuilder#connector()}</p>
 */
public interface CrankerConnector {

    /**
     * Creates the connections to the routers.
     */
    void start();

    /**
     * Starts a graceful disconnection from the routers which allows for zero-downtime deployments of components
     * that have multiple instances.
     * <p>This method will send a message to each router stating its intention to shut down, and the router will
     * stop sending new requests to this connector, and close any idle connections. When all active connections
     * have completed within timeout, it return true.</p>
     * <p>So, to perform a zero-downtime deployment where there are at least 2 services, perform a restart on
     * each instance sequentially. For each instance, first call this stop method, and when it completes, shut down
     * the target server. Then start a new instance before shutting down the next instance.</p>
     *
     * @param timeout the maximum time to wait for active requests to complete
     * @param timeUnit the time unit of the timeout argument
     * @return true if all the active requests completed within the timeout. If it returns <code>false</code> then
     * requests will still be in progress (in order to stop the active requests you can simply shut down your web service).
     */
    boolean stop(long timeout, TimeUnit timeUnit);

    /**
     * @return A unique ID assigned to this connector that is provided to the router for diagnostic reasons.
     */
    String connectorId();

    /**
     * @return Meta data about the routers that this connector is connected to. Provided for diagnostic purposes.
     */
    List<RouterRegistration> routers();
}

class CrankerConnectorImpl implements CrankerConnector {

    private volatile List<RouterRegistrationImpl> routers = Collections.emptyList();
    private final String connectorId;
    private final RouterRegistrationImpl.Factory routerConFactory;
    private final Supplier<Collection<URI>> crankerUriSupplier;
    private final RouterEventListener routerEventListener;
    private final String componentName;
    private final int routerUpdateInterval;
    private final TimeUnit timeUnit;
    private volatile ScheduledExecutorService routerUpdateExecutor;

    CrankerConnectorImpl(String connectorId, RouterRegistrationImpl.Factory routerConFactory,
                         Supplier<Collection<URI>> crankerUriSupplier, String componentName,
                         RouterEventListener routerEventListener,
                         int routerUpdateInterval, TimeUnit timeUnit) {
        this.componentName = componentName;
        this.connectorId = connectorId;
        this.routerConFactory = routerConFactory;
        this.crankerUriSupplier = crankerUriSupplier;
        this.routerEventListener = routerEventListener;
        this.routerUpdateInterval = routerUpdateInterval;
        this.timeUnit = timeUnit;
    }

    CompletableFuture<Void> updateRouters() {
        var before = this.routers;
        Collection<URI> newUris = crankerUriSupplier.get();
        var toAdd = newUris.stream()
            .filter(uri -> before.stream().noneMatch(existing -> sameRouter(uri, existing.registrationUri())))
            .map(uri -> uri.resolve("/register/?connectorInstanceID=" + urlEncode(connectorId) + "&componentName=" + urlEncode(componentName)))
            .map(routerConFactory::create).collect(toList());

        var toRemove = before.stream().filter(existing -> newUris.stream().noneMatch(newUri -> sameRouter(newUri, existing.registrationUri()))).collect(toList());

        if (!toAdd.isEmpty() || !toRemove.isEmpty()) {
            var toLeave = before.stream().filter(existing -> newUris.stream().anyMatch(newUri -> sameRouter(newUri, existing.registrationUri()))).collect(toList());
            var result = new ArrayList<>(toLeave);
            result.addAll(toAdd);
            this.routers = result;
            for (RouterRegistrationImpl newOne : toAdd) {
                newOne.start();
            }

            if (routerEventListener != null) {
                routerEventListener.onRegistrationChanged(
                    new RouterEventListener.ChangeData(
                        toAdd.stream().map(RouterRegistration.class::cast).collect(Collectors.toUnmodifiableList()),
                        toRemove.stream().map(RouterRegistration.class::cast).collect(Collectors.toUnmodifiableList()),
                        toLeave.stream().map(RouterRegistration.class::cast).collect(Collectors.toUnmodifiableList()))
                );
            }

            return CompletableFuture.allOf(toRemove.stream().map(RouterRegistrationImpl::stop).toArray(CompletableFuture<?>[]::new));
        }
        return CompletableFuture.completedFuture(null);
    }

    private static boolean sameRouter(URI registrationUrl1, URI registrationUrl2) {
        return registrationUrl1.getScheme().equals(registrationUrl2.getScheme()) && registrationUrl1.getAuthority().equals(registrationUrl2.getAuthority());
    }

    @Override
    public void start() {
        String threadName = "routerUpdateExecutor-" + connectorId();
        routerUpdateExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, threadName));
        routerConFactory.start();
        updateRouters();
        for (RouterRegistrationImpl registration : routers) {
            registration.start();
        }
        routerUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                updateRouters().get(routerUpdateInterval, timeUnit);
            } catch (Throwable e) {
                if (!(e instanceof InterruptedException)) {
                    if (routerEventListener != null) {
                        routerEventListener.onRouterDnsLookupError(e);
                    }
                }
            }
        }, routerUpdateInterval, routerUpdateInterval, timeUnit);
    }

    @Override
    public boolean stop(long timeout, TimeUnit unit) {
        try {
            doStop(Long.valueOf(timeout).intValue(), unit).get(timeout, unit);
            return true;
        } catch (Throwable throwable) {
            return false;
        }
    }

    private CompletableFuture<Void> doStop(int timeout, TimeUnit timeUnit) {
        if (this.routerUpdateExecutor == null) {
            throw new IllegalStateException("Cannot call stop() when the connector is not running. Did you call stop() twice?");
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (RouterRegistrationImpl registration : routers) {
            futures.add(registration.stop());
        }
        ScheduledExecutorService exec = this.routerUpdateExecutor;
        this.routerUpdateExecutor = null;
        exec.shutdownNow();
        futures.add(CompletableFuture.runAsync(() -> {
            try {
                exec.awaitTermination(timeout, timeUnit);
            } catch (InterruptedException ignored) {
            }
        }));
        routers.clear();
        return CompletableFuture
            .allOf(futures.toArray(CompletableFuture[]::new))
            .whenComplete((result, error) -> routerConFactory.stop());
    }

    @Override
    public String connectorId() {
        return connectorId;
    }

    @Override
    public List<RouterRegistration> routers() {
        return Collections.unmodifiableList(routers);
    }

    @Override
    public String toString() {
        return "CrankerConnector (" + connectorId + ") registered to: " + routers;
    }
}
