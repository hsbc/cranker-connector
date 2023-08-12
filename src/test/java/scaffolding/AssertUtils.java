package scaffolding;

import org.hamcrest.Matcher;

import static org.hamcrest.MatcherAssert.assertThat;

public class AssertUtils {
    public static <T> void assertEventually(Func<T> actual, Matcher<? super T> matcher) {
        assertEventually(actual, matcher, 100, 200);
    }

    public static <T> void assertEventually(Func<T> actual, Matcher<? super T> matcher, int maxAttempts, long waitMillisBetweenRetry) {
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts should be greater than 0");
        }
        AssertionError toThrow = null;
        for (int i = 0; i < maxAttempts; i++) {
            try {
                assertThat(actual.apply(), matcher);
                return;
            } catch (AssertionError e) {
                toThrow = e;
                try {
                    Thread.sleep(waitMillisBetweenRetry);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            } catch (Exception e) {
                throw new AssertionError("Error while getting value", e);
            }
        }
        throw toThrow;
    }

    public interface Func<V> {
        V apply() throws Exception;
    }
}
