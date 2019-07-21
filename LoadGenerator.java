package victordion;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * Date: July 16 2019
 *
 * A simple one thread per connection/concurrencyAtomic load generator.
 * I wrote this simply to see how a real AWS Lambda function's running time can affect TPS with a fixed concurrencyAtomic.
 * The Lambda function is the simplest one you can think of, like the following Python function. The Lambda function
 * is triggered through API Gateway. Of course you can easily change the URL endpoint to anyone you like, to say, load test a
 * HTTP base website.
 *
 * import json
 * import time
 *
 * print('Loading function')
 *
 * def lambda_handler(event, context):
 *     time.sleep(0.1) # Sleep for 100 ms
 *     return {
 *         'statusCode': 200,
 *         'headers': { 'Content-Type': 'application/json' },
 *         'body': json.dumps({ 'username': 'bob' })
 *     }
 *
 * For instance, you make the load generator to drive a steady 100 concurrecny.
 * If the Lambda function sleeps for 100 ms, and the overall p50 latency is ~130ms, you will see TPS at ~750
 *
 * The equation should approximately hold: latency_in_seconds * TPS = concurrencyAtomic.
 *
 */
class LoadGenerator {

    static class RateLimiter {
        private volatile AtomicLong bucketSizeAtomic = new AtomicLong(0);
        private volatile AtomicLong lastRefilledTimeInMillisAtomic = new AtomicLong(System.currentTimeMillis());

        private final int REFILL_SIZE_PER_SECOND;
        private final int MAX_BUCKET_SIZE;

        public RateLimiter(int maxBucketSize, int refillSizePerSecond) {
            MAX_BUCKET_SIZE = maxBucketSize;
            REFILL_SIZE_PER_SECOND = refillSizePerSecond;
        }

        public boolean tryConsume() {
            // refill
            long currentInMillis = System.currentTimeMillis();
            long elapsedInMillis = currentInMillis - lastRefilledTimeInMillisAtomic.get();
            // Do the refill only if it has been 100ms since last refill
            if (elapsedInMillis > 100) {
                long tokensToAdd = elapsedInMillis * REFILL_SIZE_PER_SECOND / 1000;
                long targetTokens = Math.min(tokensToAdd + bucketSizeAtomic.get(), MAX_BUCKET_SIZE);
                bucketSizeAtomic.set(targetTokens);
                lastRefilledTimeInMillisAtomic.set(currentInMillis);
            }

            if (bucketSizeAtomic.get() > 0) {
                bucketSizeAtomic.decrementAndGet();
                return true;
            } else {
                return false;
            }
        }
    }

    // For sending HTTP requests to the API gateway
    // Number of threads must be greater than the concurrency you want to sustain
    static ExecutorService executorService = Executors.newFixedThreadPool(200);

    // For periodically reporting metrics
    static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    static URL url;

    static  AtomicInteger requestsSentInLastReportingPeriodAtomic = new AtomicInteger(0);
    static volatile AtomicInteger requestsSucceededInLastReportingPeriodAtomic = new AtomicInteger(0);
    static volatile AtomicInteger concurrencyAtomic = new AtomicInteger(0);
    static volatile AtomicInteger everSentAtomic = new AtomicInteger(0);
    static volatile AtomicInteger everSucceededAtomic = new AtomicInteger(0);
    static volatile List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    static volatile List<Long> responseByteSizes = Collections.synchronizedList(new ArrayList<>());

    static final long REPORTING_PERIOD_IN_SECONDS = 1;
    static final int MAX_ALLOWED_CONCURRENCY = 10;
    static final int MAX_ALLOWED_TPS = 70;
    static final Semaphore CONCURRENCY_SEMAPHORE = new Semaphore(MAX_ALLOWED_CONCURRENCY);
    static final boolean PRINT_RESPONSE = false;

    static RateLimiter rateLimiter;

    /**
     *
     * @param isConcurrencyThrottling whether you want it be concurrecny throttled or TPS throttled
     */
    static private void makeRequest(boolean isConcurrencyThrottling) {
        long startTime = System.currentTimeMillis();
        BufferedReader in;
        long responseByteLength = 0;
        StringBuilder allContent = new StringBuilder();

        if (isConcurrencyThrottling) {
            try {
                CONCURRENCY_SEMAPHORE.acquire();
            } catch (InterruptedException ie) {
                String currentThreadName = Thread.currentThread().getName();
                System.out.println(String.format("Thread %s is interrupted", currentThreadName));
                return;
            }
        } else {
            boolean allowed = rateLimiter.tryConsume();
            if (!allowed) {
                return;
            }
        }

        try {
            concurrencyAtomic.incrementAndGet();
            everSentAtomic.incrementAndGet();
            requestsSentInLastReportingPeriodAtomic.incrementAndGet();
            URLConnection connection = url.openConnection();

            in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;

            while ((inputLine = in.readLine()) != null) {
                responseByteLength += inputLine.getBytes().length;
                allContent.append(inputLine);
            }

            in.close();
            everSucceededAtomic.incrementAndGet();
            requestsSucceededInLastReportingPeriodAtomic.incrementAndGet();
        } catch (Exception e) {
            System.out.println("A request failed: " + e.getMessage());
        } finally {
            if (isConcurrencyThrottling) {
                CONCURRENCY_SEMAPHORE.release();
            }

            concurrencyAtomic.decrementAndGet();
            long duration = System.currentTimeMillis() - startTime;
            latencies.add(duration);
            responseByteSizes.add(responseByteLength);
            if (PRINT_RESPONSE) {
                System.out.println(allContent.toString());
            }
        }
    }

    static public void main(String[] args) {
        System.out.println("Load generator started");
        installShutdownHook();
        scheduleMetricsReporter();

        rateLimiter = new RateLimiter(MAX_ALLOWED_TPS, MAX_ALLOWED_TPS);

        try {
            // This is an AWS API Gateway endpoint that acts as a trigger to a simple Lambda function
            url = new URL("https://abc.execute-api.us-west-2.amazonaws.com/default/jianwcui_test");
        } catch (MalformedURLException e) {
            System.out.println("URL is malformed");
            return;
        }

        while(true) {
            try {
                executorService.execute(() -> makeRequest(false));
                // I know, hacky
                Thread.yield();
            } catch (RejectedExecutionException e) {
                System.out.println("A task is rejected by executor service: " + e.getMessage() );
            }
        }
    }

    static private void scheduleMetricsReporter() {
        final Runnable reporter = () -> {
            int concurrency = concurrencyAtomic.get();
            int everSent = everSentAtomic.get();
            int everSucceeded = everSucceededAtomic.get();
            long requestsSentInLastReportingPeriod = requestsSentInLastReportingPeriodAtomic.getAndSet(0);
            long requestsSucceededInLastReportingPeriod = requestsSucceededInLastReportingPeriodAtomic.getAndSet(0);

            System.out.println("***********************");
            System.out.println("Current time: " + System.currentTimeMillis()/1000);
            System.out.println("Concurrency: " + concurrency);
            System.out.println("Ever sent: " + everSent);
            System.out.println("Ever succeeded: " + everSucceeded);
            System.out.println("Requests Sent In Last Reporting Period: " + requestsSentInLastReportingPeriod);
            System.out.println("Requests Succeeded In Last Reporting Period: " + requestsSucceededInLastReportingPeriod);

            String latencyPercentilesInString = getPercentilesInString(latencies);
            System.out.println("Latency percentiles (millisecond): " + latencyPercentilesInString);
            String responseByteSizePercentilesInString = getPercentilesInString(responseByteSizes);
            System.out.println("Response Byte Size Percentiles In String (bytes): " + responseByteSizePercentilesInString);

        };

        // Report the metrics every `REPORTING_PERIOD_IN_SECONDS` second
        long initialDelayInSeconds = 1;
        final ScheduledFuture<?> reporterHandle = scheduler.scheduleAtFixedRate(reporter, initialDelayInSeconds, REPORTING_PERIOD_IN_SECONDS, SECONDS);
    }

    static private String getPercentilesInString(List<Long> dataPoints) {
        ArrayList<Long> data = new ArrayList<>(dataPoints);
        dataPoints.clear();
        Collections.sort(data);

        double len = data.size();
        if (len < 2.0) {
            return "";
        }
        Long p50 = data.get((int) (0.5 * len));
        Long p90 = data.get((int) (0.90 * len));
        Long p99 = data.get((int) (0.99 * len));
        Long p999 = data.get((int) (0.999 * len));
        return String.format("{p50: %d, p90: %d, p99: %d, p99.9: %d}", p50, p90, p99, p999);
    }

    static private void installShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("shutdown hook is run");
                executorService.shutdownNow();
                scheduler.shutdownNow();
                try {
                    executorService.awaitTermination(5, TimeUnit.SECONDS);
                    scheduler.awaitTermination(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    System.out.println("Shutdown hook is interrupted: " + e.getCause().getMessage());
                }
            }
        });
    }
}
