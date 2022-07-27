package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;

import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Main {
    private static boolean isRunning = true;

    public static void main(String[] args) throws Exception {

        DriverConfigLoader loader = DriverConfigLoader.fromClasspath("application.conf");

        try (CqlSession session = CqlSession.builder()
                .withConfigLoader(loader)
                .withKeyspace("my_keyspace")
                .build()) {

            PreparedStatement ps = session.prepare(SimpleStatement.newInstance("SELECT * FROM my_table WHERE partition_id = ?"));

            AtomicLong counter = new AtomicLong(0);
            final int partition = Integer.parseInt(System.getProperty("test.partition", "0"));
            int THREADS = Integer.parseInt(System.getProperty("test.threads", "8"));
            BoundStatement bs = ps.bind(partition);

            // producer that submits the queries.
            for (int i = 0; i < THREADS; i++) {
                Thread t = new Thread(() -> {
                    while (isRunning) {
                        ResultSet rs = session.execute(bs);
                        long c = 0;
                        for (Row row : rs) {
                            c++;
                        }
                        counter.addAndGet(c);
                    }
                });
                t.start();
            }

            Thread logger = new Thread(() -> {
                long start = System.currentTimeMillis();
                long prevCounter = counter.get();
                while (isRunning) {
                    try {
                        Thread.sleep(5000);
                        long elapsed = System.currentTimeMillis() - start;
                        long currentCounter = counter.get();
                        double rate = (currentCounter - prevCounter) / (elapsed / 1000.0);
                        prevCounter = currentCounter;
                        start = System.currentTimeMillis();
                        System.out.println("Current rate: " + rate + " rows/second");
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            logger.start();
            long start = System.currentTimeMillis();
            Thread.sleep(60000);
            isRunning = false;
            logger.join();

            long elapsed = System.currentTimeMillis() - start;
            double rate = counter.get() / (elapsed / 1000.0);
            System.out.println("Rate: " + rate + " rows/second");

            System.exit(0);
        }
    }
}
