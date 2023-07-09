package app_perfClient;

import logger.LogSetup;
import org.apache.log4j.Level;
import shared.PerformanceMonitor;

import java.io.File;
import java.io.IOException;

public class PerformanceTester {
    private static final String KEY_PERFORMANCE = "performance_key";

    public static void main(String[] args) throws IOException, InterruptedException {
        new LogSetup("logs/testing/test.log", Level.ERROR);

        if (args.length != 6) {
            System.out.println("Usage: <address> <port> <payload_size> <num_commands> <num_get_clients> <num_put_clients>");
            System.exit(1);
        }

        String address = args[0];
        int port = Integer.parseInt(args[1]);
        int payloadSize = Integer.parseInt(args[2]);
        int numCommands = Integer.parseInt(args[3]);
        int numGetClients = Integer.parseInt(args[4]);
        int numPutClients = Integer.parseInt(args[5]);

        PerformanceMonitor monitor = new PerformanceMonitor();
        PerfClient[] clients = new PerfClient[numGetClients + numPutClients];
        for (int i = 0; i < numGetClients; i++) {
            clients[i] = new GetClient(address, port, numCommands, KEY_PERFORMANCE, monitor);
        }
        for (int i = 0; i < numPutClients; i++) {
            clients[i + numGetClients] = new PutClient(address, port, numCommands, KEY_PERFORMANCE, monitor, payloadSize);
        }

        Thread[] threads = new Thread[clients.length];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(clients[i]);
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        monitor.dumpStatistics(new File("perf.csv"));
    }
}
