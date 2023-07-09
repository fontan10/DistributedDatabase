package app_kvServer;

import shared.PerformanceMonitor;
import shared.messages.IKVMessage.StatusType;
import shared.messages.SocketMessenger;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.IOException;
import java.net.Socket;

import static shared.messages.IKVMessage.StatusType.*;

public class KVThread extends Thread {
    private static final Logger LOGGER = Logger.getRootLogger();

    private final Socket socket;
    private final KVServer server;
    private final SocketMessenger socketMessenger;

    public KVThread(Socket socket, KVServer server) throws IOException {
        LOGGER.info("thread alive: " + this.getId());
        this.socket = socket;
        this.server = server;
        this.socketMessenger = new SocketMessenger(socket);
    }

    public void run() {
        while (true) {
            try {
                KVMessage request = null;
                try {
                    request = this.socketMessenger.receiveMessage();
                } catch (IllegalArgumentException e) {
                    // this is not a fatal error, let's let the server keep accepting input from this client
                    LOGGER.warn("unknown message status type received", e);
                    this.socketMessenger.sendMessage(new KVMessage(e.getMessage(), FAILED));
                    continue;
                }

                if (request == null) {
                    break;
                }

                PerformanceMonitor.Operation perfOp = null;
                long start = System.nanoTime();
                switch (request.getStatus()) {
                    case PUT: {
                        perfOp = PerformanceMonitor.Operation.PUT;
                        boolean isDeleteRequest = request.getValue() == null;
                        try {
                            LOGGER.info("Request received: PUT <" + request.getKey()  + "> <" + request.getValue() + ">");
                            String previousValue = this.server.putKV(request.getKey(), request.getValue());
                            StatusType status = isDeleteRequest ? (previousValue == null ? DELETE_ERROR : DELETE_SUCCESS) : (previousValue == null ? PUT_SUCCESS : PUT_UPDATE);
                            KVMessage response = new KVMessage(request.getKey(), request.getValue(), status);
                            LOGGER.info("Sending response: " + status.name() + " <" + request.getKey() + "> <" + request.getValue() + ">");
                            this.socketMessenger.sendMessage(response);
                        } catch (IOException e) {
                            // TODO: This block is not covered by tests. A mocking library is required to mock errors.
                            LOGGER.error("Unexpected error for PUT <" + request.getKey() + "> <" + request.getValue() + ">", e);
                            StatusType status = isDeleteRequest ? DELETE_ERROR : PUT_ERROR;
                            KVMessage response = new KVMessage(request.getKey(), request.getValue(), status);
                            this.socketMessenger.sendMessage(response);
                        }
                        break;
                    }

                    case GET: {
                        perfOp = PerformanceMonitor.Operation.GET;
                        try {
                            LOGGER.info("Request received: GET <" + request.getKey() + ">");
                            String value = this.server.getKV(request.getKey());
                            StatusType status = value == null ? GET_ERROR : GET_SUCCESS;
                            KVMessage response = new KVMessage(request.getKey(), value, status);
                            LOGGER.info("Sending response: " + status.name() + " <" + request.getKey()  + "> <" + value + ">");
                            this.socketMessenger.sendMessage(response);
                        } catch (IOException e) {
                            LOGGER.error("Unexpected error for GET <" + request.getKey() + ">", e);
                            KVMessage response = new KVMessage(request.getKey(), GET_ERROR);
                            this.socketMessenger.sendMessage(response);
                        }
                        break;
                    }

                    case KEYRANGE: {
                        LOGGER.info("Request received: KEYRANGE");
                        // TODO: populate key with actual metadata
                        String key = "metadata";
                        KVMessage response = new KVMessage(key, KEYRANGE_SUCCESS);
                        this.socketMessenger.sendMessage(response);
                        break;
                    }

                    default: {
                        LOGGER.error("Response StatusType provided for request: " + request.getStatus() + " only GET and PUT are accepted.");
                    }
                }
                long end = System.nanoTime();

                if (server.performanceMonitor != null && perfOp != null) {
                    server.performanceMonitor.storeOperation(perfOp, end - start);
                }
            } catch (IOException socketMessengerException) {
                LOGGER.error("I/O error communicating via socket messenger: ", socketMessengerException);
                try {
                    this.socket.close();
                } catch (IOException closeSocketException) {
                    LOGGER.error("I/O error when attempting to close socket: ", closeSocketException);
                }
                break;
            }
        }
        LOGGER.info("thread finished: " + this.getId());
    }
}
