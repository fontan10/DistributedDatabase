package app_kvServer;

import com.fasterxml.jackson.databind.ObjectMapper;
import logger.LogSetup;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.PerformanceMonitor;
import sun.misc.Signal;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

public class KVServer implements IKVServer, Runnable {
    private static final String STORAGE_FILE = "store.json";
    private static final String EMPTY_KV_STORE = "{}";
    private static final Logger LOGGER = Logger.getRootLogger();

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Object lock = new Object();
    private final InetAddress address;
    private final int port;
    private final InetSocketAddress ecsSocketAddress;
    private final InetSocketAddress ringSocketAddress;
    private final int cacheSize;
    private final CacheStrategy cacheStrategy;
    private final String storageFilePath;

    private Socket ecsSocket;
    private ServerSocket serverSocket;
    private boolean running;
    PerformanceMonitor performanceMonitor;
    /**
     * The latch will be zero when the socket is alive.
     */
    private CountDownLatch runningLatch;

    /**
     * Start KV Server at given address and port
     *
     * @param address   given address for storage server to operate
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(InetAddress address, int port, InetSocketAddress ecsSocketAddress, InetSocketAddress ringSocketAddress,
                    int cacheSize, String strategy, String directory, boolean performanceMonitorEnabled) throws UnknownHostException {
        if (address == null) {
            LOGGER.error("Address address not found.");
            throw new UnknownHostException(ecsSocketAddress.getHostName());
        }

        this.address = address;
        this.port = port;
        this.ecsSocketAddress = ecsSocketAddress;
        this.ringSocketAddress = ringSocketAddress;
        this.cacheSize = cacheSize;
        this.cacheStrategy = CacheStrategy.valueOf(strategy);
        this.running = false;
        this.storageFilePath = String.valueOf(Paths.get(directory, STORAGE_FILE));

        if (performanceMonitorEnabled) {
            this.performanceMonitor = new PerformanceMonitor();
        }

        this.runningLatch = new CountDownLatch(1);
    }

    /**
     * Start KV Server at given port on localhost
     *
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) throws UnknownHostException {
        this(InetAddress.getLocalHost(), port, new InetSocketAddress(InetAddress.getLocalHost(), port + 1), new InetSocketAddress(InetAddress.getLocalHost(), port + 2),
                cacheSize, strategy, System.getProperty("user.dir"), false);
    }

    public KVServer(InetSocketAddress ecsSocketAddress, int cacheSize, String strategy) throws UnknownHostException {
        this(ecsSocketAddress.getAddress(), ecsSocketAddress.getPort() - 1, new InetSocketAddress(ecsSocketAddress.getAddress(), ecsSocketAddress.getPort()), new InetSocketAddress(ecsSocketAddress.getAddress(), ecsSocketAddress.getPort() + 1),
                cacheSize, strategy, System.getProperty("user.dir"), false);
    }

    @Override
    public InetAddress getAddress() {
        return address;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public InetSocketAddress getEcsSocketAddress() {
        return this.ecsSocketAddress;
    }

    @Override
    public InetSocketAddress getRingSocketAddress() {
        return this.ringSocketAddress;
    }

    @Override
    public String getHostname() {
        return address.getHostName();
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return cacheStrategy;
    }

    @Override
    public int getCacheSize() {
        return cacheSize;
    }

    @Override
    public boolean inStorage(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean inCache(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getKV(String key) throws IOException {
        synchronized (lock) {
            HashMap<String, String> store = this.objectMapper.readValue(new File(this.storageFilePath), HashMap.class);
            return store.get(key);
        }
    }

    @Override
    public String putKV(String key, String value) throws IOException {
        synchronized (lock) {
            HashMap<String, String> store = this.objectMapper.readValue(new File(this.storageFilePath), HashMap.class);
            String previousValue = store.put(key, value);
            this.objectMapper.writeValue(new File(this.storageFilePath), store);
            return previousValue;
        }
    }

    @Override
    public void clearCache() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() {
        File file = new File(this.storageFilePath);
        if (file.isFile()) {
            file.delete();
        }
    }

    @Override
    public void run() {
        this.running = initializeServer();

        if (this.running) {
            runningLatch.countDown();
        }

        while (this.running) {
            try {
                new KVThread(this.serverSocket.accept(), this).start();
            } catch (IOException e) {
                LOGGER.error("error accepting connection", e);
            }
        }
    }

    @Override
    public void kill() {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() {
        this.running = false;
        this.runningLatch = new CountDownLatch(1);
        try {
            this.serverSocket.close();
            this.ecsSocket.close();
        } catch (IOException e) {
            LOGGER.error("error closing server socket", e);
        }
    }

    private boolean initializeServer() {
        try {
            KVECSThread kvecsThread = new KVECSThread(this.ecsSocketAddress, this);

            try {
                kvecsThread.connect();
            } catch (IOException e) {
                LOGGER.error("could not open ECS socket", e);
                return false;
            }

            this.ecsSocket = kvecsThread.getECSSocket();

            this.serverSocket = new ServerSocket(this.port);
            LOGGER.info("server listening on " + this.address.toString() + ":" + this.port);
            try {
                File storageFile = new File(this.storageFilePath);
                if (!storageFile.isFile()) {
                    FileWriter fileWriter = new FileWriter(storageFile);
                    fileWriter.write(EMPTY_KV_STORE);
                    fileWriter.close();
                }
            } catch (IOException e) {
                LOGGER.error("could not initialize storage file", e);
                return false;
            }
            return true;
        } catch (IOException e) {
            LOGGER.error("could not open server socket", e);
            return false;
        }
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("KVServer", options);
    }

    private static Options initializeOptions() {
        Options options = new Options();

        Option port = Option.builder("p")
                .longOpt("port")
                .hasArg()
                .desc("Sets the port of the server")
                .type(int.class)
                .required()
                .build();

        Option ecsAddressPort = Option.builder("e")
                .longOpt("ecs")
                .hasArg()
                .desc("Sets the IP and port of the ECS, e.g., “localhost:5001”.")
                .type(String.class)
                .required()
                .build();

        options.addOption(port)
                .addOption(ecsAddressPort)
                .addOption("a", true, "Which address the server should listen to. Default is localhost")
                .addOption("r", true, "Which address and port the server should listen to for its ring neighbours, e.g., “localhost:5002”.")
                .addOption("d", true, "Directory for storing cache files. Default is current directory")
                .addOption("l", true, "Relative path of the logfile, e.g., “echo.log”. Default is current directory")
                .addOption("ll", true, "Loglevel, e.g., INFO, ALL, …. Default is ALL")
                .addOption("h", false, "Display the help")
                .addOption("m", false, "Enable performance monitoring");

        return options;
    }

    public CountDownLatch getRunningLatch() {
        return runningLatch;
    }

    private static InetSocketAddress inetSocketAddressFromString(String string) {
        String[] addressPort = string.split(":");
        if (addressPort.length != 2) {
            return null;
        }

        InetSocketAddress inetSocketAddress = null;
        try {
            inetSocketAddress = new InetSocketAddress(addressPort[0], Integer.parseInt(addressPort[1]));
        } catch (Exception e) {
            return null;
        }

        return inetSocketAddress;
    }

    public static void main(String[] args) {
        Options options = initializeOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            printUsage(options);
            System.exit(1);
        }

        if (cmd.hasOption("h")) {
            printUsage(options);
            System.exit(1);
        }

        String sEcsSocketAddress = cmd.getOptionValue("e");
        InetSocketAddress ecsSocketAddress = inetSocketAddressFromString(sEcsSocketAddress);
        if (ecsSocketAddress == null) {
            System.out.println("invalid argument e");
            printUsage(options);
            System.exit(1);
        }

        String sRingSocketAddress = cmd.getOptionValue("r", ecsSocketAddress.getAddress() + ":" + (ecsSocketAddress.getPort() + 1));
        InetSocketAddress ringSocketAddress = inetSocketAddressFromString(sRingSocketAddress);
        if (ringSocketAddress == null) {
            System.out.println("invalid argument r");
            printUsage(options);
            System.exit(1);
        }

        int port = 0;
        try {
            port = Integer.parseInt(cmd.getOptionValue("p"));
        } catch (NumberFormatException e) {
            System.out.println("argument p must be an integer");
            printUsage(options);
            System.exit(1);
        }

        InetAddress address = null;
        try {
            address = InetAddress.getByName(cmd.getOptionValue("a", "localhost"));
        } catch (UnknownHostException e) {
            System.out.println("invalid address");
            e.printStackTrace();
            System.exit(1);
        }

        String directory = cmd.getOptionValue("d", System.getProperty("user.dir"));
        String logfilePath = cmd.getOptionValue("l", String.valueOf(Paths.get(System.getProperty("user.dir"), "echo.log")));
        String logLevel = cmd.getOptionValue("ll", "ALL");
        boolean perfEnabled = cmd.hasOption("m");

        if (!LogSetup.isValidLevel(logLevel)) {
            System.out.println("invalid logLevel");
            System.out.println(LogSetup.getPossibleLogLevels());
            System.exit(1);
        }

        try {
            new LogSetup(logfilePath, Level.toLevel(logLevel));
        } catch (IOException e) {
            System.out.println("unable to initialize logger");
            e.printStackTrace();
            System.exit(1);
        }

        try {
            final int CACHE_SIZE = 0;
            final String CACHE_STRATEGY = "None";

            KVServer kvServer = new KVServer(
                    address,
                    port,
                    ecsSocketAddress,
                    ringSocketAddress,
                    CACHE_SIZE,
                    CACHE_STRATEGY,
                    directory,
                    perfEnabled
            );

            Thread serverThread = new Thread(kvServer);
            serverThread.start();

            // Add a signal handler for SIGUSR1 to dump performance statistics
            if (perfEnabled) {
                Signal.handle(new Signal("USR1"), signal -> {
                    try {
                        kvServer.performanceMonitor.dumpStatistics(new File("server_perf.log"));
                    } catch (IOException e) {
                        LOGGER.error("failed to dump stats", e);
                    }
                });
            }
        } catch (NumberFormatException e) {
            System.out.println("port or cache_size was not a number");
            e.printStackTrace();
            System.exit(1);
        } catch (IllegalArgumentException e) {
            System.out.println("cache_strategy must be either FIFO, LRU, LFU, or None");
            e.printStackTrace();
            System.exit(1);
        } catch (UnknownHostException e) {
            System.out.println("Address " + ecsSocketAddress + " not found.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
