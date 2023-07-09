package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ConnectionTest {
    private static KVServer kvServer;
    private static ECSClient ecsClient;
    private static final int KV_SERVER_PORT = 50000;

    @BeforeClass
    public static void beforeAll() throws InterruptedException {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);

            ecsClient = new ECSClient(InetAddress.getLocalHost(), KV_SERVER_PORT + 1);
            Thread ecsThread = new Thread(ecsClient);
            ecsThread.start();

            kvServer = new KVServer(KV_SERVER_PORT, 10, "FIFO");
            kvServer.clearStorage();
            Thread serverThread = new Thread(kvServer);
            serverThread.start();
            assertTrue("server should start up", kvServer.getRunningLatch().await(500, TimeUnit.MILLISECONDS));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterAll() {
        kvServer.close();
        ecsClient.stop();
    }


    @Test
    public void testConnectionSuccess() {

        Exception ex = null;

        KVStore kvClient = new KVStore("localhost", KV_SERVER_PORT);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }

    @Test
    public void testUnknownHost() {
        Exception ex = null;
        KVStore kvClient = new KVStore("unknown", KV_SERVER_PORT);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof UnknownHostException);
    }

    @Test
    public void testIllegalPort() {
        Exception ex = null;
        try {
            KVStore kvClient = new KVStore("localhost", 123456789);
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof IllegalArgumentException);
    }
}
