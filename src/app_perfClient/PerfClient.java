package app_perfClient;

import client.KVCommInterface;
import client.KVStore;
import shared.PerformanceMonitor;

public abstract class PerfClient implements Runnable {
    protected final KVCommInterface kvStore;
    /**
     * Number of commands to issue.
     */
    private final int commandCount;
    protected final String key;
    private final PerformanceMonitor monitor;

    public PerfClient(String address, int port, int commandCount, String key, PerformanceMonitor monitor) {
        this.monitor = monitor;
        this.kvStore = new KVStore(address, port);
        this.key = key;
        this.commandCount = commandCount;
    }

    @Override
    public void run() {
        try {
            this.kvStore.connect();

            for (int issued = 0; issued < commandCount; issued++) {
                long start = System.nanoTime();
                this.send();
                long end = System.nanoTime();

                this.monitor.storeOperation(getOperation(), end - start);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract PerformanceMonitor.Operation getOperation();
    protected abstract void send() throws Exception;
}
