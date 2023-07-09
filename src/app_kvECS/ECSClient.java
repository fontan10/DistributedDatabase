package app_kvECS;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.NavigableMap;

import ecs.ECSNode;
import ecs.ECSRing;
import logger.LogSetup;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.HashUtils;

public class ECSClient implements Runnable {
    private static final Logger LOGGER = Logger.getRootLogger();

    private final ECSRing<Object> ring = new ECSRing<Object>();
    private final ServerSocket serverSocket;
    private boolean running;

    public ECSClient(InetAddress address, int port) throws IOException {
        this.serverSocket = new ServerSocket(port, 50, address);
    }

    public ECSClient(int port) throws IOException {
        this.serverSocket = new ServerSocket(port, 50, InetAddress.getLocalHost());
    }

    public void run() {
        this.running = true;

        while (this.running) {
            try {
                Socket socket = this.serverSocket.accept();
                LOGGER.info("Accepted connection from " + socket.getRemoteSocketAddress());
                new ECSThread(socket, this).start();
            } catch (IOException e) {
                LOGGER.error("Error accepting connection", e);
            }
        }
    }

    public void stop() {
        if (!this.running) {
            LOGGER.warn("Tried to stop ECS, but it is not running");
            return;
        }

        this.running = false;
    }

    public void clearNodes() {
        this.ring.clearNodes();
    }

    public String addNode(String host, Integer port) throws IllegalArgumentException, UnknownHostException {
        String startHash = this.ring.addNode(host, port);
        String nextKey = this.ring.getMap().higherKey(startHash);
        String endHash = nextKey != null ? HashUtils.hashSubtractOne(nextKey) : HashUtils.hashSubtractOne(this.ring.getMap().firstKey());

        return startHash + "," + endHash + "," + host + ":" + port;
    }

    /**
     * @return semicolon separate triples kr-from, kr-to, ip:port
     */
    public String buildMetadata() {
        StringBuilder metadata = new StringBuilder();
        NavigableMap<String, ECSNode<Object>> map = this.ring.getMap();
        String[] keys = map.keySet().toArray(new String[0]);
        System.out.println(keys.length);

        for (int i = 0; i < keys.length; i++) {
            String currentKey = keys[i];
            String nextKey = i + 1 < keys.length ? keys[i + 1] : keys[0]; // handle wrap around
            metadata.append(currentKey + ",");
            metadata.append(HashUtils.hashSubtractOne(nextKey) + ",");
            metadata.append(map.get(currentKey).getNodeIpAndPort() + ";");
        }

        return metadata.toString();
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
                .desc("Sets the port of the ECS")
                .type(int.class)
                .required()
                .build();

        options.addOption(port)
                .addOption("a", true, "The address ECS will listen on. Default is localhost")
                .addOption("l", true, "Relative path of the logfile, e.g., “echo.log”. Default is current directory")
                .addOption("ll", true, "Loglevel, e.g., INFO, ALL, …. Default is ALL")
                .addOption("h", false, "Display this help text");

        return options;
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

        String logfilePath = cmd.getOptionValue("l", String.valueOf(Paths.get(System.getProperty("user.dir"), "echo.log")));
        String logLevel = cmd.getOptionValue("ll", "ALL");

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
            ECSClient ecs = new ECSClient(address, port);
            ecs.run();
        } catch (IOException e) {
            LOGGER.error("Failed to start ECS", e);
            System.exit(1);
        }
    }
}
