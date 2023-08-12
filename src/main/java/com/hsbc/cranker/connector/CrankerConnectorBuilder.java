package com.hsbc.cranker.connector;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.hsbc.cranker.connector.HttpUtils.createHttpClientBuilder;

/**
 * A builder of {@link CrankerConnector} objects
 */
public class CrankerConnectorBuilder {

    /**
     * cranker protocol 1.0
     */
    public final static String CRANKER_PROTOCOL_1 = "cranker_1.0";

    /**
     * cranker protocol 3.0
     */
    public final static String CRANKER_PROTOCOL_3 = "cranker_3.0";

    private final static List<String> SUPPORTED_CRANKER_PROTOCOLS = List.of(CRANKER_PROTOCOL_3, CRANKER_PROTOCOL_1);

    private Supplier<Collection<URI>> crankerUris;
    private String domain = "*";
    private String route;
    private URI target;
    private int slidingWindowSize = 2;
    private String componentName = "cranker-connector";
    private HttpClient client;
    private final String connectorId = UUID.randomUUID().toString();
    private RouterEventListener routerEventListener;
    private ProxyEventListener proxyEventListener;
    private int routerUpdateInterval = 1;
    private TimeUnit routerUpdateTimeUnit = TimeUnit.MINUTES;
    private int routerDeregisterTimeout = 1;
    private TimeUnit routerDeregisterTimeUnit = TimeUnit.MINUTES;
    private List<String> preferredProtocols = List.of(CRANKER_PROTOCOL_3, CRANKER_PROTOCOL_1);

    /**
     * <p>Specifies the source of the router URIs to register with, for example: <code>builder.withRouterUris(RegistrationUriSuppliers.dnsLookup(URI.create("wss://router.example.org")))</code></p>
     * <p>This will be frequently called, allowing the routers to expand and shrink at runtime.</p>
     * <p>See the {@link RegistrationUriSuppliers} classes for some pre-built suppliers.</p>
     *
     * @param supplier A function returning router websocket URIs to connect to. Note that no path is required.
     * @return This builder
     */
    public CrankerConnectorBuilder withRouterUris(Supplier<Collection<URI>> supplier) {
        this.crankerUris = supplier;
        return this;
    }

    /**
     * <p>Registers all A-records associated with the given URIs and uses those to connect to the cranker routers. This
     * DNS lookup happens periodically, so if the DNS is changed then the connected routers will auto-update.</p>
     * <p>This is just a shortcut for <code>withRouterUris(RegistrationUriSuppliers.dnsLookup(uris))</code></p>
     *
     * @param uris The URIs to perform a DNS lookup on, and then connect to. Example: <code>URI.create("wss://router.example.org");</code>
     * @return This builder
     */
    public CrankerConnectorBuilder withRouterLookupByDNS(URI... uris) {
        return withRouterUris(RegistrationUriSuppliers.dnsLookup(uris));
    }

    /**
     * The interval for updating routers, by evaluating what supplied in {{@link #withRouterUris(Supplier)}}
     * or {{@link #withRouterLookupByDNS(URI...)}}. If the routers updated, it will auto register to the new added routers
     * and de-register from the removed routers.
     *
     * @param interval interval for updating router
     * @param timeUnit time unit for updating router
     * @return This builder
     */
    public CrankerConnectorBuilder withRouterUpdateInterval(int interval, TimeUnit timeUnit) {
        this.routerUpdateInterval = interval;
        this.routerUpdateTimeUnit = timeUnit;
        return this;
    }

    /**
     * <p>Timeout setting for de-registering connector from router. Normally router will disconnect the websocket connections after
     * inflight requests completed, but it's possible that it takes too long or hanging. When timeout happen, connector disconnect
     * the websockets, which may causing inflight request broken.</p>
     * @param timeout timeout
     * @param timeUnit timeUnit
     * @return This builder
     */
    public CrankerConnectorBuilder withRouterDeregisterTimeout(int timeout, TimeUnit timeUnit) {
        this.routerDeregisterTimeout = timeout;
        this.routerDeregisterTimeUnit = timeUnit;
        return this;
    }

    /**
     * Specifies domain that this connector is serving. For example, if this connector serves
     * requests from <code><a href="https://my-domain.com/hello">...</a></code> then the domains is <code>"my-domain.com"</code>
     * <p>
     * The default value of domains is  <code>"*"</code>, which mean it can support requests for any domains
     *
     * @param domain The domain of the web app to serve
     * @return This builder
     */
    public CrankerConnectorBuilder withDomain(String domain) {
        if (domain == null || domain.trim().length() == 0) throw new IllegalArgumentException("domain cannot be empty");
        this.domain = domain;
        return this;
    }

    /**
     * <p>Specifies which route, or path prefix, this connector is serving. For example, if this connector serves
     * requests from <code>/my-app/*</code> then the route is <code>my-app</code></p>
     *
     *
     * <p>If this connector serves <code>/my-app/instance-a/*</code> then the route is <code>my-app/instance-a</code>. But
     * this will require cranker support <code>/</code> in route.</p>
     *
     * @param route The path prefix of the web app, or <code>*</code> to forward everything that isn't matched
     *              by any other connectors
     * @return This builder
     */
    public CrankerConnectorBuilder withRoute(String route) {
        if (route == null) throw new IllegalArgumentException("route cannot be null");
        if ("*".equals(route) || route.matches("[a-zA-Z0-9/_-]+")) {
            this.route = route;
            return this;
        } else {
            throw new IllegalArgumentException("Routes must contain only letters, numbers, underscores or hyphens");
        }
    }

    /**
     * Specifies the web server that calls should be proxied to
     *
     * @param target The root URI of the web server to send requests to
     * @return This builder
     */
    public CrankerConnectorBuilder withTarget(URI target) {
        if (target == null) throw new IllegalArgumentException("Target cannot be null");
        this.target = target;
        return this;
    }

    /**
     * Optionally sets the number of idle connections per router.
     *
     * @param slidingWindowSize the number of idle connections to connect to each router.
     * @return This builder
     */
    public CrankerConnectorBuilder withSlidingWindowSize(int slidingWindowSize) {
        this.slidingWindowSize = slidingWindowSize;
        return this;
    }

    /**
     * Sets the name of this component so that the router is able to expose in diagnostics the component name of
     * connections that are connected.
     *
     * @param componentName The name of the target component, for example &quot;my-app-service&quot;
     * @return This builder
     */
    public CrankerConnectorBuilder withComponentName(String componentName) {
        this.componentName = componentName;
        return this;
    }

    /**
     * Sets a listener that is notified whenever the router registrations change.
     * <p>As some changes may occur due to external actions (e.g. a change to DNS if {@link RegistrationUriSuppliers#dnsLookup(Collection)} is used)
     * it may be useful to listen for changes and simply log the result for informational purposes.</p>
     *
     * @param listener The listener to be called when registration changes
     * @return This builder
     */
    public CrankerConnectorBuilder withRouterRegistrationListener(RouterEventListener listener) {
        this.routerEventListener = listener;
        return this;
    }

    /**
     * Sets a listener that is notified whenever there is requests proxying.
     *
     * @param listener The listener to be called when proxying requests
     * @return This builder
     */
    public CrankerConnectorBuilder withProxyEventListener(ProxyEventListener listener) {
        this.proxyEventListener = listener;
        return this;
    }

    /**
     * Optionally sets an HTTP client used. If not set, then a default one will be used.
     * <p>An HTTP builder suitable for using can be created with {@link #createHttpClient(boolean)}</p>
     *
     * @param client The client to use to connect to the target server
     * @return This builder
     */
    public CrankerConnectorBuilder withHttpClient(HttpClient client) {
        this.client = client;
        return this;
    }

    /**
     * Optionally sets preferred cranker protocol version, which used for cranker protocol negotiation.
     * If not set, then default [&quot;cranker_3.0&quot;, &quot;cranker_1.0&quot;] will be used.
     * Cranker server will use the first protocol it supported.
     *
     * <p>&quot;cranker_3.0&quot; is for multiplexing which supporting flow control</p>
     *
     * @param preferredProtocols The preferred cranker protocols to be used in negotiation.
     *                           see {@link #CRANKER_PROTOCOL_3} {@link #CRANKER_PROTOCOL_1} for valid values.
     * @return This builder
     */
    public CrankerConnectorBuilder withPreferredProtocols(List<String> preferredProtocols) {
        if (preferredProtocols == null || preferredProtocols.size() == 0) {
            throw new IllegalArgumentException("preferredProtocols should not be empty.");
        }
        for (String protocol : preferredProtocols) {
            if (!SUPPORTED_CRANKER_PROTOCOLS.contains(protocol)) {
                throw new IllegalArgumentException("protocol " + protocol + " not valid, these are the valid protocols: " + String.join(",", SUPPORTED_CRANKER_PROTOCOLS));
            }
        }
        this.preferredProtocols = preferredProtocols;
        return this;
    }

    /**
     * Creates a new HTTP Client builder that is suitable for use in the connector.
     *
     * @param trustAll If true, then any SSL certificate is allowed.
     * @return An HTTP Client builder
     */
    public static HttpClient.Builder createHttpClient(boolean trustAll) {
        return createHttpClientBuilder(trustAll)
            .followRedirects(HttpClient.Redirect.NEVER);
    }

    /**
     * @return A new connector builder
     */
    public static CrankerConnectorBuilder connector() {
        return new CrankerConnectorBuilder();
    }

    /**
     * Creates a connector. Consider using {@link #start()} instead.
     *
     * @return A (non-started) connector
     * @see #start()
     */
    public CrankerConnector build() {

        if (route == null) throw new IllegalStateException("A route must be specified");
        if (target == null) throw new IllegalStateException("A target must be specified");
        if (componentName == null) throw new IllegalStateException("A componentName must be specified");

        HttpClient clientToUse = client != null ? client : createHttpClient(false).build();
        ProxyEventListener proxyEventListenerToUse = proxyEventListener != null ? proxyEventListener : new ProxyEventListener(){};
        var factory = new RouterRegistrationImpl.Factory(preferredProtocols, clientToUse, domain, route, slidingWindowSize, target, routerEventListener, proxyEventListenerToUse);
        return new CrankerConnectorImpl(connectorId, factory, crankerUris, componentName, routerEventListener,
            this.routerUpdateInterval, this.routerUpdateTimeUnit, this.routerDeregisterTimeout, this.routerDeregisterTimeUnit);
    }

    /**
     * Creates and then starts a connector
     *
     * @return A started connector
     */
    public CrankerConnector start() {
        CrankerConnector cc = build();
        cc.start();
        return cc;
    }


}
