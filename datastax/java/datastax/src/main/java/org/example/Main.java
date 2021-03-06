package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Main {
    private static boolean isRunning = true;

    public static void main(String[] args) throws Exception {
        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("cassandra", 9042))
                .withKeyspace("my_keyspace")
                .withLocalDatacenter("datacenter1")
                .build();
        PreparedStatement ps = session.prepare(SimpleStatement.newInstance("SELECT * FROM my_table where partition_id = ?"));

        AtomicLong counter = new AtomicLong(0);
        final int ID = Integer.parseInt(System.getProperty("test.id", "1"));
        final int THREADS = Integer.parseInt(System.getProperty("test.threads", "1"));
        final int HELPER_THREADS = Integer.parseInt(System.getProperty("test.helper.threads", "1"));
        final int PERMITS = Integer.parseInt(System.getProperty("test.permits", "128"));
        Executor executor = Executors.newFixedThreadPool(HELPER_THREADS);
        Semaphore semaphore = new Semaphore(PERMITS);

        // producer that submits the queries.
        for (int i = 0; i < THREADS; i++) {
            Thread producer = new Thread(() -> {
                while (isRunning) {
                    semaphore.acquireUninterruptibly();
                    session.executeAsync(ps.bind(ID))
                            .thenComposeAsync(rs -> countRows(rs, 0, executor), executor)
                            .whenCompleteAsync((count, error) -> {
                                if (error != null) {
                                    System.err.println(error.getMessage());
                                    isRunning = false;
                                } else {
                                    counter.addAndGet(count);
                                }
                                semaphore.release();
                            }, executor);
                }
            });
            producer.start();
        }

        long start = System.currentTimeMillis();
        Thread.sleep(60000);

        isRunning = false;
        semaphore.acquireUninterruptibly(PERMITS);

        long elapsed = System.currentTimeMillis() - start;
        double rate = counter.get() / (elapsed / 1000.0);
        System.out.println("Rate: " + rate + " rows/second");

        System.exit(0);
    }

    private static CompletionStage<Long> countRows(final AsyncResultSet resultSet, long previousPagesCount, final Executor executor) {
        long count = previousPagesCount;
        for (Row row : resultSet.currentPage()) {
            for (int i = 0, n = row.size(); i < n; i++) {
                row.getObject(i);
            }
            count += 1;
        }
        if (resultSet.hasMorePages()) {
            final long COUNT = count;
            return resultSet.fetchNextPage().thenComposeAsync(rs -> countRows(rs, COUNT, executor), executor);
        } else {
            return CompletableFuture.completedFuture(count);
        }
    }
}
