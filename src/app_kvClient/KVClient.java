package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.IKVMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;

import static shared.messages.KVMessage.MAX_KEY_BYTE_LENGTH;
import static shared.messages.KVMessage.MAX_VAL_BYTE_LENGTH;

public class KVClient implements IKVClient, Runnable {
    private static final Logger LOGGER = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";

    private boolean stop = false;

    private KVCommInterface kvStore = null;
    private String serverAddress;
    private int serverPort;

    public KVClient(KVCommInterface kvStore) {
        this.kvStore = kvStore;
    }

    public KVClient() {
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        connect(hostname, port);
    }

    @Override
    public KVCommInterface getStore() {
        return kvStore;
    }

    @Override
    public void run() {
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        while (!stop) {
            printPrompt();

            try {
                String cmdLine = stdin.readLine();
                handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        if (cmdLine == null) {
            stop = true;
            return;
        }

        String[] tokens = cmdLine.stripLeading().split("\\s+", 3); // limit 3 because value can contain whitespace

        if (tokens.length == 0) {
            printError("Unknown command");
            printHelp();
            return;
        }

        switch (tokens[0]) {
            case "connect":
                connectHandler(tokens);
                break;
            case "disconnect":
                disconnect();
                break;
            case "put":
                putHandler(tokens);
                break;
            case "get":
                getHandler(tokens);
                break;
            case "logLevel":
                logLevelHandler(tokens);
                break;
            case "help":
                printHelp();
                break;
            case "quit":
                stop = true;
                disconnect();
                printPrompt("Application exit!");
                break;
            default:
                printError("Unknown command");
                printHelp();
        }
    }

    private void connectHandler(String[] tokens) {
        if (tokens.length == 3) {
            try {
                serverAddress = tokens[1];
                serverPort = Integer.parseInt(tokens[2]);
                connect(serverAddress, serverPort);
                LOGGER.info("Connection successful: " + serverAddress + " / " + serverPort);
            } catch (NumberFormatException nfe) {
                printError("No valid address. Port must be a number!");
                LOGGER.info("Unable to parse argument <port>", nfe);
            } catch (UnknownHostException e) {
                printError("Unknown Host!");
                LOGGER.info("Unknown Host!", e);
            } catch (Exception e) {
                printError("Could not establish connection!");
                LOGGER.warn("Could not establish connection!", e);
            }
        } else {
            printInvalidNumParams();
        }
    }

    private void connect(String address, Integer port) throws Exception {
        KVCommInterface tempKVStore = new KVStore(address, port);
        tempKVStore.connect();

        if (this.kvStore != null) {
            this.kvStore.disconnect();
        }

        this.kvStore = tempKVStore;
    }

    private void disconnect() {
        if (this.kvStore != null) {
            this.kvStore.disconnect();
            this.kvStore = null;
        }
    }

    private void putHandler(String[] args) {
        if (kvStore == null) {
            printError("Please connect to an address and port");
            return;
        }

        if (args.length != 2 && args.length != 3) {
            printInvalidNumParams();
            return;
        }

        String key = args[1];
        String val = args.length == 3 && args[2].length() > 0 ? args[2] : null;

        if (!validateByteLength(key, MAX_KEY_BYTE_LENGTH) || !validateByteLength(val, MAX_VAL_BYTE_LENGTH)) {
            return;
        }

        try {
            IKVMessage kvMsg = kvStore.put(key, val);
            IKVMessage.StatusType status = kvMsg.getStatus();

            switch (status) {
                case PUT_SUCCESS: {
                    String msg = "Inserted <" + key + "> <" + val + ">";
                    printPrompt(msg);
                    LOGGER.info(msg);
                    break;
                }
                case PUT_UPDATE: {
                    String msg = "Updated <" + key + "> <" + val + ">";
                    printPrompt(msg);
                    LOGGER.info(msg);
                    break;
                }
                case PUT_ERROR: {
                    String msg = "Put <" + key + "> <" + val + "> unsuccessful";
                    printPrompt(msg);
                    LOGGER.info(msg);
                    break;
                }
                case DELETE_SUCCESS: {
                    String msg = "Deleted <" + key + ">";
                    printPrompt(msg);
                    LOGGER.info(msg);
                    break;
                }
                case DELETE_ERROR: {
                    String msg = "Delete <" + key + "> unsuccessful";
                    printPrompt(msg);
                    LOGGER.info(msg);
                    break;
                }
                default: {
                    String msg = "Received unexpected StatusType " + status.name() + " in response";
                    LOGGER.warn(msg);
                    printError(msg);
                }
            }

        } catch (Exception e) {
            String msg;
            if (val != null) {
                msg = "Unsuccessful put of key <" + key + "> and value <" + val + ">";
            } else {
                msg = "Unsuccessful delete of key <" + key + ">";
            }
            LOGGER.warn(msg, e);
            printError(msg);
        }
    }

    private void getHandler(String[] args) {
        if (kvStore == null) {
            printError("Please connect to an address and port");
            return;
        }

        if (args.length != 2) {
            printInvalidNumParams();
            return;
        }

        String key = args[1];

        if (!validateByteLength(key, MAX_KEY_BYTE_LENGTH)) {
            return;
        }

        try {
            IKVMessage kvMsg = kvStore.get(key);
            IKVMessage.StatusType status = kvMsg.getStatus();

            if (!key.equals(kvMsg.getKey())) {
                LOGGER.warn("Received kvMsg is for key <" + kvMsg.getKey() + "> instead of <" + key + ">");
                throw new Exception();
            }

            switch (status) {
                case GET_SUCCESS: {
                    printPrompt(kvMsg.getValue());
                    LOGGER.info("GET_SUCCESS: " + kvMsg.getValue());
                    break;
                }
                case GET_ERROR: {
                    printError("Value for key <" + key + "> not found");
                    LOGGER.info("GET_ERROR: Get of <" + key + "> not successful");
                    break;
                }
                default: {
                    String msg = "Received unexpected StatusType " + status.name() + " in response";
                    LOGGER.warn(msg);
                    printError(msg);
                }
            }
        } catch (Exception e) {
            String msg = "Unsuccessful get of key <" + key + ">";
            LOGGER.warn(msg, e);
            printError(msg);
        }
    }

    private void logLevelHandler(String[] args) {
        if (args.length == 2) {
            String level = setLevel(args[1]);
            if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                printError("No valid log level!");
                printPossibleLogLevels();
            } else {
                printPrompt("Log level changed to level " + level);
            }
        } else {
            printError("Invalid number of parameters!");
        }
    }

    private String setLevel(String levelString) {
        if (levelString.equals(Level.ALL.toString())) {
            LOGGER.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            LOGGER.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            LOGGER.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            LOGGER.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            LOGGER.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            LOGGER.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            LOGGER.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private boolean validateByteLength(String str, int maxBytes) {
        String encoding = "UTF-8";
        try {
            if (str != null && str.getBytes(encoding).length > maxBytes) {
                String msg = "<" + str + "> is more than max length of " + maxBytes + " Bytes";
                LOGGER.info(msg);
                printError(msg);
                return false;
            }
        } catch (UnsupportedEncodingException e) {
            LOGGER.warn("Could not encode value <" + str + "> as " + encoding, e);
            printError("Unsupported character in value " + str + ". Cannot encode value as " + encoding);
            return false;
        }

        return true;
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
        sb.append(PROMPT).append("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");
        sb.append(PROMPT).append("put <key> [value]");
        sb.append("\t\t inserts or updates the key-value pair \n");
        sb.append("\t\t deletes the entry for key if value unspecified \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t retrieves the value for key \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPrompt() {
        System.out.print(PROMPT);
    }

    private void printPrompt(String msg) {
        System.out.println(PROMPT + msg);
    }

    private void printError(String error) {
        printPrompt("Error! " + error);
    }

    private void printInvalidNumParams() {
        printError("Invalid number of parameters!");
    }

    private void printPossibleLogLevels() {
        printPrompt("Possible log levels are:");
        printPrompt("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    /**
     * Main entry point for the echo server application.
     *
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ALL);
        } catch (IOException e) {
            System.out.println("unable to initialize logger");
            e.printStackTrace();
            System.exit(1);
        }

        KVClient client = new KVClient();

        client.run();
    }
}
