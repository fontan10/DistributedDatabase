package app_kvECS;

import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.SocketMessenger;
import shared.UnexpectedMessageException;

import java.io.IOException;
import java.net.Socket;

public class ECSThread extends Thread {
    private static final Logger LOGGER = Logger.getRootLogger();

    private final Socket socket;
    private final ECSClient ecs;
    private final SocketMessenger socketMessenger;

    // Becomes true when CONNECT_SUCCESS has been sent to KVServer
    private boolean ready;
    private String clientAddress;
    private String ringAddress;

    public ECSThread(Socket socket, ECSClient ecs) throws IOException {
        this.socket = socket;
        this.ecs = ecs;
        this.socketMessenger = new SocketMessenger(socket);

        this.ready = false;
    }

    @Override
    public void run() {
        try (Socket socket = this.socket) {
            waitForConnect();
            // TODO: add this KVServer to hash ring
            sendConnectSuccess();

            while (this.ready) {
                KVMessage msg = this.socketMessenger.receiveMessage();
                if (msg.getStatus() == IKVMessage.StatusType.DISCONNECT) {
                    this.ready = false;
                    break;
                }

                // TODO: Handle other messages (i.e. TRANSFER_SUCCESS)
            }
        } catch (IOException e) {
            LOGGER.error("Communications error occurred with client", e);
        } catch (UnexpectedMessageException e) {
            LOGGER.warn("KVServer sent unexpected messages", e);
        }
    }

    private void waitForConnect() throws IOException, UnexpectedMessageException {
        // TODO: explore adding a timeout for this
        KVMessage connectMsg = this.socketMessenger.receiveMessage();
        if (connectMsg.getStatus() != IKVMessage.StatusType.CONNECT) {
            throw new UnexpectedMessageException(connectMsg,
                    "expected CONNECT as the first message");
        }

        this.clientAddress = connectMsg.getKey();
        this.ringAddress = connectMsg.getValue();
    }

    private void sendConnectSuccess() throws IOException {
        System.out.println("sent connect");
        KVMessage msg = new KVMessage(IKVMessage.StatusType.CONNECT_SUCCESS);
        this.socketMessenger.sendMessage(msg);
        this.ready = true;
    }

    public String getClientAddress() {
        return this.clientAddress;
    }

    public String getRingAddress() {
        return this.ringAddress;
    }
}
