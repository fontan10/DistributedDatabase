package app_kvServer;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    }

    /**
     * Get the address the server is listening on
     *
     * @return address
     */
    public InetAddress getAddress();

    /**
     * Get the port number of the server
     *
     * @return port number
     */
    public int getPort();

    /**
     * Get the address and port of the ECS
     *
     * @return InetSocketAddress
     */
    public InetSocketAddress getEcsSocketAddress();

    /**
     * Get the address and port for listening to ring neighbour
     *
     * @return InetSocketAddress
     */
    public InetSocketAddress getRingSocketAddress();


    /**
     * Get the hostname of the server
     *
     * @return hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     *
     * @return cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     *
     * @return cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     *
     * @return true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     *
     * @return true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     *
     * @return value associated with key
     * @throws Exception when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     *
     * @throws Exception when key not in the key range of the server
     */
    public String putKV(String key, String value) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();
}
