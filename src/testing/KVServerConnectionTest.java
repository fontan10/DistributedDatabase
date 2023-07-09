package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.*;


public class KVServerConnectionTest {
    private static ECSClient ecsClient;
    private static final int ECS_PORT = 50020;
    private static InetAddress ecsAddress;
    private static InetSocketAddress ecsSocketAddress;

    @BeforeClass
    public static void beforeAll() {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);

            try {
                ecsAddress = InetAddress.getLocalHost();
                ecsSocketAddress = new InetSocketAddress(InetAddress.getLocalHost(), ECS_PORT);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }

            ecsClient = new ECSClient(ecsAddress, ECS_PORT);
            Thread ecsThread = new Thread(ecsClient);
            ecsThread.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterAll() {
        ecsClient.stop();
    }


    @Test
    public void testConnectionSuccess() {
        Exception ex = null;
        try {
            KVServer kvServer = new KVServer(ecsSocketAddress, 10, "FIFO");
            kvServer.clearStorage();
            Thread serverThread = new Thread(kvServer);
            serverThread.start();

            // allow time for thread to create connection to ECS
            Thread.sleep(100);
            kvServer.close();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }

    @Test(expected = UnknownHostException.class)
    public void testUnknownHost() throws UnknownHostException {
        KVServer kvServer = new KVServer(new InetSocketAddress("unknown", ECS_PORT), 10, "FIFO");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalPort() throws UnknownHostException {
        KVServer kvServer = new KVServer(new InetSocketAddress("localhost", 123456789), 10, "FIFO");
        kvServer.run();
    }
}
