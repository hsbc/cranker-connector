package manual;

import com.hsbc.cranker.connector.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is for simulating a slow client which reading the response body slowly.
 * it's for testing the flow control / back pressure
 */
public class ManualSlowClientTest {

    private static final Logger log = LoggerFactory.getLogger(ManualSlowClientTest.class);

    public static void main(String[] args) throws InterruptedException {
        final HttpClient client = HttpUtils.createHttpClientBuilder(true).build();

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("https://localhost:12345/big.log"))
            .header("user-agent", "jdk-httpclient")
            .build();

        CountDownLatch latch = new CountDownLatch(1);

        AtomicInteger received = new AtomicInteger(0);
        HttpResponse.BodyHandler<Void> bodyHandler = responseInfo -> HttpResponse.BodySubscribers.fromSubscriber(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                this.subscription.request(1);
            }

            @Override
            public void onNext(List<ByteBuffer> items) {
                for (ByteBuffer item : items) {
                    received.addAndGet(item.remaining());
                    final CharBuffer decode = StandardCharsets.UTF_8.decode(item);
                    System.out.println(decode);
                }
                 try {
                     Thread.sleep(1000L);
                 } catch (InterruptedException e) {
                     throw new RuntimeException(e);
                 }
                this.subscription.request(1);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        final long start = System.currentTimeMillis();
        client.sendAsync(request, bodyHandler);
        final boolean await = latch.await(1, TimeUnit.HOURS);
        log.info("completed in {} ms, success={}, received {} bytes", (System.currentTimeMillis() - start), await, received.get());
    }

}
