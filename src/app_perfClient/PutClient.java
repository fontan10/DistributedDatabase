package app_perfClient;

import shared.PerformanceMonitor;

import java.util.Random;

public class PutClient extends PerfClient {
    private final String payload;

    public PutClient(String address, int port, int commandCount, String key, PerformanceMonitor monitor, int payloadSize) {
        super(address, port, commandCount, key, monitor);

        Random random = new Random();
        this.payload = random.ints('a', 'z' + 1)
                .limit(payloadSize)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    @Override
    protected PerformanceMonitor.Operation getOperation() {
        return PerformanceMonitor.Operation.PUT;
    }

    @Override
    protected void send() throws Exception {
        this.kvStore.put(key, payload);
    }
}
