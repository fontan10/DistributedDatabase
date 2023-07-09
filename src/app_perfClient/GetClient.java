package app_perfClient;

import shared.PerformanceMonitor;

public class GetClient extends PerfClient {
    public GetClient(String address, int port, int commandCount, String key, PerformanceMonitor monitor) {
        super(address, port, commandCount, key, monitor);
    }

    @Override
    protected PerformanceMonitor.Operation getOperation() {
        return PerformanceMonitor.Operation.GET;
    }

    @Override
    protected void send() throws Exception {
        this.kvStore.get(this.key);
    }
}
