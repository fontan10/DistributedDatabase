package shared;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PerformanceMonitor {
    public enum Operation {
        GET,
        PUT,
    }

    private final HashMap<Operation, ArrayList<Long>> operationTimes;

    public PerformanceMonitor() {
        this.operationTimes = new HashMap<>();
        for (Operation o : Operation.values()) {
            this.operationTimes.put(o, new ArrayList<>());
        }
    }

    public synchronized void storeOperation(Operation op, long duration) {
        operationTimes.get(op).add(duration);
    }

    public synchronized void dumpStatistics(File output) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(output));
        for (Operation op : Operation.values()) {
            List<Long> times = operationTimes.get(op);
            System.out.println(op.name() + " " + times.size());
            double average = (double) times.stream().reduce(Long::sum).orElse(0L) / times.size();
            writer.write(op.name() + "," + average + "\n");
        }
        writer.close();
    }
}
