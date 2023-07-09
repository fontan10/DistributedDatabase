package ecs;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class ECSNode<E> {
    private final InetSocketAddress inetSocketAddress;
    private final String nodeIpAndPort;
    private final E data;

    public ECSNode(String host, Integer port) throws IllegalArgumentException, UnknownHostException {
        this(host, port, null);
    }

    public ECSNode(String host, Integer port, E data) throws IllegalArgumentException, UnknownHostException {
        this.inetSocketAddress = new InetSocketAddress(host, port);
        if (inetSocketAddress.isUnresolved()) {
            throw new UnknownHostException(host);
        }

        this.nodeIpAndPort = this.inetSocketAddress.getHostName() + ":" + this.inetSocketAddress.getPort();
        this.data = data;
    }

    public InetSocketAddress getSocketAddress() {
        return inetSocketAddress;
    }

    public String getNodeIpAndPort() {
        return this.nodeIpAndPort;
    }

    public E getData() {
        return data;
    }
}
