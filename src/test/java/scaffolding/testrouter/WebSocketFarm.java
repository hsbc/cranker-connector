package scaffolding.testrouter;

import io.muserver.Mutils;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.function.ObjIntConsumer;

class WebSocketFarm {
    private static final Logger log = LoggerFactory.getLogger(WebSocketFarm.class);

    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<RouterSocket>> sockets = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<RouterSocket> catchAll = new ConcurrentLinkedQueue<>();
    private final HashedWheelTimer wheelTimer = new HashedWheelTimer(new DefaultThreadFactory("mucranker-socket-acquirer"));
    private final int socketAcquireRetries;
    private final long durationBetweenRetriesInMillis;
    private final AtomicInteger idleCount = new AtomicInteger(0);
    private final ConcurrentHashMap.KeySetView<DarkHost, Boolean> darkHosts = ConcurrentHashMap.newKeySet();

    public WebSocketFarm(int maxAttempts, long durationBetweenRetriesInMillis) {
        this.socketAcquireRetries = maxAttempts;
        this.durationBetweenRetriesInMillis = durationBetweenRetriesInMillis;
    }

    public void start() {
        wheelTimer.start();
    }

    public void stop() {
        wheelTimer.stop();
        for (RouterSocket routerSocket : catchAll) {
            routerSocket.socketSessionClose();
        }
        for (ConcurrentLinkedQueue<RouterSocket> queue : sockets.values()) {
            for (RouterSocket routerSocket : queue) {
                routerSocket.socketSessionClose();
            }
        }
    }

    public int idleCount() {
        return this.idleCount.get();
    }

    public boolean removeWebSocket(String route, RouterSocket socket) {
        boolean removed = false;
        if (socket.isCatchAll()) {
            removed = catchAll.remove(socket);
        } else {
            ConcurrentLinkedQueue<RouterSocket> routerSockets = sockets.get(route);
            if (routerSockets != null) {
                removed = routerSockets.remove(socket);
            }
        }
        if (removed) {
            idleCount.decrementAndGet();
        }
        return removed;
    }

    public void addWebSocket(String route, RouterSocket socket) {
        ConcurrentLinkedQueue<RouterSocket> queue;
        if (socket.isCatchAll()) {
            queue = catchAll;
        } else {
            sockets.putIfAbsent(route, new ConcurrentLinkedQueue<>());
            queue = sockets.get(route);
        }
        if (queue.offer(socket)) {
            idleCount.incrementAndGet();
        }
    }

    /**
     * Attempts to get a websocket to send a request on.
     *
     * @param attemptNum The 0-based attempt at getting a socket for this call. If this value exceeds the max attempts
     *                   (as defined be {@link CrankerRouterBuilder#withConnectorAcquireAttempts(int, long)})
     *                   then the
     * @param target     The full path of the request, for example <code>/some-service/blah</code> (in which a socket
     *                   for the <code>some-service</code> route would be looked up)
     * @param onSuccess  A callback if this is successful. If there is a socket already waiting then this is executed
     *                   immediately on the same thread. If no sockets are available for the given target, then it will
     *                   be scheduled to try again based on the timeout value on {@link CrankerRouterBuilder#withConnectorAcquireAttempts(int, long)}
     *                   on a different thread.
     * @param onFailure  Called with number of attempts made to get a socket if no socket can be acquired
     */
    public void acquireSocket(int attemptNum, String target, ObjIntConsumer<RouterSocket> onSuccess, IntConsumer onFailure) {
        String route = resolveRoute(target);
        ConcurrentLinkedQueue<RouterSocket> routerSockets = sockets.getOrDefault(route, catchAll);
        RouterSocket socket;
        if (darkHosts.isEmpty()) {
            socket = routerSockets.poll();
        } else {
            socket = getNonDarkSocket(routerSockets);
        }
        if (socket != null) {
            idleCount.decrementAndGet();
            onSuccess.accept(socket, attemptNum);
        } else {
            if (attemptNum > socketAcquireRetries) {
                onFailure.accept(attemptNum);
            } else {
                wheelTimer.newTimeout(timeout -> {
                        if (!timeout.isCancelled()) {
                            acquireSocket(attemptNum + 1, target, onSuccess, onFailure);
                        }
                    },
                    durationBetweenRetriesInMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    private RouterSocket getNonDarkSocket(ConcurrentLinkedQueue<RouterSocket> routerSockets) {
        for (RouterSocket candidate : routerSockets) {
            if (candidate.isDarkModeOn(this.darkHosts)) {
                continue;
            }
            boolean removed = routerSockets.remove(candidate);
            if (removed) {
                return candidate;
            }
        }
        return null;
    }

    public void deRegisterSocket(String target, String remoteAddr, String connectorInstanceID) {
        log.debug("Going to deregister targetName=" + target + " and the targetAddr=" + remoteAddr + " and the connectorInstanceID=" + connectorInstanceID);
        ConcurrentLinkedQueue<RouterSocket> routerSockets = sockets.getOrDefault(target, catchAll);
        routerSockets.forEach(a -> removeSockets(connectorInstanceID, a));
    }

    private void removeSockets(String connectorInstanceID, RouterSocket routerSocket) {
        String currentConnectorInstanceID = routerSocket.connectorInstanceID();
        if (currentConnectorInstanceID.equals(connectorInstanceID)) {
            boolean removed = removeWebSocket(routerSocket.route, routerSocket);
            if (removed) {
                routerSocket.socketSessionClose();
            }
        }
    }

    private String resolveRoute(String target) {
        if (target.split("/").length >= 2) {
            return target.split("/")[1];
        } else {
            // It's either root target, or blank target
            return "*";
        }
    }


    ConcurrentLinkedQueue<RouterSocket> getCatchAll() {
        return catchAll;
    }

    ConcurrentHashMap<String, ConcurrentLinkedQueue<RouterSocket>> getSockets() {
        return sockets;
    }

    void enableDarkMode(DarkHost darkHost) {
        Mutils.notNull("darkHost", darkHost);
        boolean added = darkHosts.add(darkHost);
        if (added) {
            log.info("Enabled dark mode for " + darkHost);
        } else {
            log.info("Requested dark mode for " + darkHost + " but it was already in dark mode, so doing nothing.");
        }
    }

    void disableDarkMode(DarkHost darkHost) {
        Mutils.notNull("darkHost", darkHost);
        boolean removed = darkHosts.remove(darkHost);
        if (removed) {
            log.info("Disabled dark mode for " + darkHost);
        } else {
            log.info("Requested to disable dark mode for " + darkHost + " but it was not in dark mode, so doing nothing.");
        }
    }

    Set<DarkHost> getDarkHosts() {
        return Collections.unmodifiableSet(new HashSet<>(darkHosts));
    }

}