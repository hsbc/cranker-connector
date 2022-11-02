cranker-connector
=================

A cranker connector that has no external dependencies. Requires JDK11 or later.

Background
----------

Cranker is a load-balancing reverse proxy designed for systems with many HTTP services that need to
be exposed at a single endpoint. It was designed for fast-moving teams that need to deploy early and often
with no central configuration needed when a new service is introduced or an existing one is upgraded.

The key difference with other reverse proxies and load balancers is that each service connects via a "connector"
to one or more routers. This connection between connector and router is a websocket, and crucially it means
the service is self-configuring; the router knows where a service is and if it is available by the fact that
it has an active websocket connection for a service.

Although there are websockets in the middle, from the point of view of clients and the target microservices,
everything is standard HTTP, and any HTTP libraries can be used.

For Java-based services, a connector can be embedded into the service by using a connector library, such
as this one.

Usage
-----

In this scenario, you have a microservice that you want to expose on a list of existing cranker routers.

1. Add a dependency to this library. Note: this will be deployed to Nexus in the near future.
2. In your service, start a web server using your preferred framework. This will only be accessed over
   a local connection, so the port number is not important (so port 0 is recommended), and you can bind to the
   localhost network interface which is recommended for security reasons.
3. Create a `CrankerConnector` object, passing it the router URLs and your service's URL.
    ````java
    URI targetServiceUri = startedWebServerUri();
    
    CrankerConnector connector = CrankerConnectorBuilder.connector()
        .withRouterLookupByDNS(URI.create("wss://my-router.example.org:8008"))
        .withRoute("path-prefix")
        .withTarget(targetServiceUri)
        .start();
    ````

Your service will now be available on the cranker router.

If you have multiple routers then a single domain name can point to all instances of your router. The `withRouterLookupByDNS`
method will result in a periodic DNS lookup to resolve the router IP addresses. Note that using this approach will use
IP-address URLs which may affect SNI and/or hostname verification. Using fixed DNS names if that's an issue or see the SSL
section below for a workaround.

Listening to events
-------------------

If you want to know things like why your connector cannot connect to a router, or see when the registration URLs
change, you can listen:

````java
CrankerConnectorBuilder.connector()
    .withRouterRegistrationListener(new RouterEventListener() {
       public void onRegistrationChanged(ChangeData data) {
           log.info("Router registration changed: " + data);
       }
       public void onSocketConnectionError(RouterRegistration router, Throwable exception) {
           log.warn("Error connecting to " + router, exception);
       }
    })
````

SSL Configuration
-----------------

This library establishes connections to one or more routers, and one local target HTTP server. These connections can
use HTTPS/WSS or plain HTTP/WS. If using a secure connection, then the default JDK parameters are used. However, if
self-signed certs are used (which is common for localhost communications), you can specify a client that trusts
all SSL certs on the `CrankerConnectorBuilder`.

Note that if you need to disable hostname verification (e.g. when connecting to routers via IP addresses) then this needs
to be set as a system property before any HttpClient code is used by setting a system property.

The following shows a full example:

````java
public static void main(String[] args) {
    System.setProperty("jdk.internal.httpclient.disableHostnameVerification", "true");

    URI targetServiceUri = startedWebServerUri();
    
    CrankerConnector connector = CrankerConnectorBuilder.connector()
        .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
        .withRouterLookupByDNS(URI.create("wss://my-router.example.org:8008"))
        .withRoute("path-prefix")
        .withTarget(targetServiceUri)
        .start();
}
````

Zero downtime deployments
-------------------------

As long as you have at least 2 instances of your connected service, you can perform a zero downtime
deployment by simply stopping and restarting one instance at a time. In order to ensure that any
in-flight requests complete before your application is shutdown, call `stop(long timeout, TimeUnit timeUnit)`
on the connector object. This will deregister connector from router and wait for the active request completed.

* It will return true if all active requests have completed within timeout.
* If it returns false on timeout, requests will still be in progress. In order to stop the active requests
  you can simply shut down your web service.

Here is an example
```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    if (!connector.stop(10, TimeUnit.SECONDS)) log.info("Killing active requests");
    targetServer.stop();
}));
```

Release Note
------------

Take a look at [here](./RELEASE.md)
