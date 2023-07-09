package client;

import org.apache.log4j.Logger;

import ecs.ECSNode;
import ecs.ECSRing;
import shared.UnexpectedMessageException;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.*;

public class KVStore implements KVCommInterface {
    private static final Logger LOGGER = Logger.getRootLogger();

    private final String address;
    private final int port;

    private final ECSRing<Object> ring;

    /**
     * Initialize KVStore with address and port of a KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {
        this.address = address;
        this.port = port;

        this.ring = new ECSRing<>();
    }

    @Override
    public void connect() throws IOException {
        this.ring.addNode(address, port);
    }

    @Override
    public void disconnect() {
        this.ring.clearNodes();
    }

    @Override
    public IKVMessage put(String key, String value) throws UnexpectedMessageException, IOException {
        KVMessage putRequest = new KVMessage(key, value, IKVMessage.StatusType.PUT);

        try (KVNodeComm node = getNodeForKey(key)) {
            return tryAndReceiveWithExpBackoff(node, putRequest);
        }
    }

    @Override
    public IKVMessage get(String key) throws UnexpectedMessageException, IOException {
        KVMessage getRequest = new KVMessage(key, IKVMessage.StatusType.GET);

        try (KVNodeComm node = getNodeForKey(key)) {
            return tryAndReceiveWithExpBackoff(node, getRequest);
        }
    }

    private KVNodeComm getNodeForKey(String key) throws IOException {
        ECSNode<Object> node = this.ring.getNodeForKey(key);
        if (node == null) {
            throw new IOException("no node exists");
        }

        return new KVNodeComm(node.getSocketAddress());
    }

    private void updateMetadata(KVNodeComm node) throws UnexpectedMessageException, IOException {
        IKVMessage response = node.getMetadata();
        String metadata = response.getValue();

        String[] nodes = metadata.split(";");
        this.ring.clearNodes();
        for (String nodeData : nodes) {
            // nodeData should look like: <from>,<to>,<address:port>
            String[] parts = nodeData.split(",");
            try {
                String fromHash = parts[0];
                String[] nodeAddress = parts[2].split(":");

                this.ring.addNode(fromHash, nodeAddress[0], Integer.parseInt(nodeAddress[1]));
            } catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
                throw new UnexpectedMessageException(response, "improperly formatted node data in metadata");
            }
        }
    }

    private IKVMessage tryAndReceiveWithExpBackoff(KVNodeComm node, KVMessage request) throws UnexpectedMessageException, IOException {
        for (int iteration = 0; ; iteration++) {
            long milliseconds = (long) (Math.pow(2, iteration - 1));
            try {
                Thread.sleep(milliseconds);
            } catch (InterruptedException e) {
                LOGGER.warn("Thread.sleep interrupted", e);
            }

            IKVMessage response = node.sendAndReceiveMessage(request);

            if (response.getStatus() == IKVMessage.StatusType.SERVER_STOPPED ||
                    response.getStatus() == IKVMessage.StatusType.SERVER_WRITE_LOCK) {
                continue;
            }

            if (response.getStatus() == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                updateMetadata(node);
                continue;
            }

            return response;
        }
    }
}
