package client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import shared.UnexpectedMessageException;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.SocketMessenger;

public class KVNodeComm implements Closeable {
    private final Socket socket;
    private final SocketMessenger socketMessenger;

    public KVNodeComm(InetSocketAddress address) throws IOException {
        this.socket = new Socket(address.getAddress(), address.getPort());
        this.socketMessenger = new SocketMessenger(this.socket);
    }

    @Override
    public void close() throws IOException {
        this.socket.close();
    }

    public IKVMessage sendAndReceiveMessage(KVMessage message) throws IOException {
        socketMessenger.sendMessage(message);
        return socketMessenger.receiveMessage();
    }

    public IKVMessage getMetadata() throws UnexpectedMessageException, IOException {
        IKVMessage message = sendAndReceiveMessage(new KVMessage(IKVMessage.StatusType.KEYRANGE));
        if (message.getStatus() != IKVMessage.StatusType.KEYRANGE_SUCCESS) {
            throw new UnexpectedMessageException(message, "expected KEYRANGE_SUCCESS in response to KEYRANGE");
        }

        return message;
    }
}
