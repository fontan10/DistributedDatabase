package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.SocketMessenger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import static shared.messages.IKVMessage.StatusType.*;

public class KVECSThread extends Thread {
    private static final Logger LOGGER = Logger.getRootLogger();

    private Socket socket;
    private SocketMessenger socketMessenger;
    private final InetSocketAddress ecsSocketAddress;
    private final KVServer server;

    public KVECSThread(InetSocketAddress ecsSocketAddress, KVServer server) throws IOException {
        LOGGER.info("KVECSThread alive: " + this.getId());
        this.ecsSocketAddress = ecsSocketAddress;
        this.server = server;
    }

    public Socket getECSSocket() {
        return socket;
    }

    public boolean connect() throws IOException {
        try {
            this.socket = new Socket(this.ecsSocketAddress.getAddress(), this.ecsSocketAddress.getPort());
            this.socketMessenger = new SocketMessenger(socket);
        } catch (IOException e) {
            LOGGER.error("I/O error creating ECS socket.", e);
            throw e;
        }

        // TODO:
        // send CONNECT
        // receive METADATA_UPDATE
        // receive CONNECT_SUCCESS
        return true;
    }

    public boolean disconnect() {
        // TODO:
        // send DISCONNECT
        // receive TRANSFER
        // begin TRANSFER protocol
        // send TRANSFER_SUCCESS
        // maybe receive UPDATE_METADATA ?
        // receive DISCONNECT_SUCCESS
        return true;
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

                switch (request.getStatus()) {
                    default: {
                        LOGGER.error("Response StatusType provided for request: " + request.getStatus() + " only GET and PUT are accepted.");
                    }
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
        LOGGER.info("KVECSThread finished: " + this.getId());
    }
}
