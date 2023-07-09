package testing;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import app_kvECS.ECSClient;
import logger.LogSetup;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.SocketMessenger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class ECSConnectionTest {
    private static ECSClient ecs;
    private static final InetAddress ADDRESS = InetAddress.getLoopbackAddress();
    private static final int PORT = 50005;

    @BeforeClass
    public static void beforeAll() throws IOException {
        new LogSetup("logs/testing/test.log", Level.WARN);
        ecs = new ECSClient(ADDRESS, 50005);
        Thread ecsThread = new Thread(ecs);
        ecsThread.start();
    }

    @AfterClass
    public static void afterAll() {
        ecs.stop();
    }

    @Test
    public void testConnectionCanBeMade() throws IOException {
        Socket mockSocket = new Socket(ADDRESS, PORT);
        assertTrue(mockSocket.isConnected());
        mockSocket.close();
    }

    @Test(expected = IOException.class)
    public void testConnectionNoHandshakeFails() throws IOException {
        Socket mockSocket = new Socket(ADDRESS, PORT);
        SocketMessenger messenger = new SocketMessenger(mockSocket);

        // The first message sent to the ECS should be CONNECT
        messenger.sendMessage(new KVMessage("address:port", "from,to", IKVMessage.StatusType.TRANSFER_SUCCESS));

        // The socket should now be closed
        messenger.receiveMessage();
    }

    @Test
    public void testConnectionSendsConnectSuccess() throws IOException {
        Socket mockSocket = new Socket(ADDRESS, PORT);
        SocketMessenger messenger = new SocketMessenger(mockSocket);

        // The first message sent to the ECS should be CONNECT
        messenger.sendMessage(new KVMessage("client:clientport", "ring:ringport",
                IKVMessage.StatusType.CONNECT));
        KVMessage response = messenger.receiveMessage();

        assertEquals(response.getStatus(), IKVMessage.StatusType.CONNECT_SUCCESS);
    }
}
