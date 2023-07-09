package ecs;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import shared.HashUtils;

public class ECSRing<E> {
    private final NavigableMap<String, ECSNode<E>> hashRing;

    public ECSRing() {
        this.hashRing = new TreeMap<>();
    }

    /**
     * @return the hash for this node
     */
    public String addNode(String host, Integer port) throws IllegalArgumentException, UnknownHostException {
        ECSNode<E> node = new ECSNode<E>(host, port);
        String nodeIpAndPort = node.getNodeIpAndPort();
        String startHash = HashUtils.md5(nodeIpAndPort);

        this.hashRing.put(startHash, node);
        return startHash;
    }

    public void addNode(String hash, String host, Integer port) throws IllegalArgumentException, UnknownHostException {
        this.hashRing.put(hash, new ECSNode<E>(host, port));
    }

    public void clearNodes() {
        this.hashRing.clear();
    }

    public NavigableMap<String, ECSNode<E>> getMap() {
        return Collections.unmodifiableNavigableMap(this.hashRing);
    }

    public ECSNode<E> getNodeByHash(String hash) {
        return this.hashRing.get(hash);
    }

    public ECSNode<E> getNodeForKey(String key) {
        if (this.hashRing.isEmpty()) {
            return null;
        }
        String hash = HashUtils.md5(key);
        Map.Entry<String, ECSNode<E>> floorEntry = this.hashRing.floorEntry(hash);
        return floorEntry != null ? floorEntry.getValue() : this.hashRing.lastEntry().getValue();
    }
}
