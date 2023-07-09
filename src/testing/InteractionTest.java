package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.*;

import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class InteractionTest {

    private static KVServer kvServer;
    private static ECSClient ecsClient;
    private KVStore kvClient;
    private static final int KV_SERVER_PORT = 50010;

    @BeforeClass
    public static void beforeAll() throws InterruptedException {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);

            ecsClient = new ECSClient(KV_SERVER_PORT + 1);
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

    @Before
    public void beforeEach() throws IOException {
        kvClient = new KVStore("localhost", KV_SERVER_PORT);
        kvClient.connect();
    }

    @After
    public void afterEach() {
        kvClient.disconnect();
    }

    @Test
    public void testPut() throws Exception {
        String key = "foo2";
        String value = "bar2";

        IKVMessage response = kvClient.put(key, value);

        assertEquals(response.getStatus(), StatusType.PUT_SUCCESS);
        assertEquals(response.getKey(), key);
        assertEquals(response.getValue(), value);
    }

    @Test(expected = IOException.class)
    public void testPutDisconnected() throws Exception {
        kvClient.disconnect();
        String key = "foo";
        String value = "bar";

        kvClient.put(key, value);
    }

    @Test
    public void testUpdate() throws Exception {
        String key = "updateTestValue";
        String initialValue = "initial";
        String updatedValue = "updated";

        IKVMessage putResponse = kvClient.put(key, initialValue);
        assertEquals(putResponse.getValue(), initialValue);

        IKVMessage updateResponse = kvClient.put(key, updatedValue);
        assertEquals(updateResponse.getStatus(), StatusType.PUT_UPDATE);
        assertEquals(updateResponse.getKey(), key);
        assertEquals(updateResponse.getValue(), updatedValue);
    }

    @Test
    public void testDeleteWithNullString() throws Exception {
        String key = "deleteTestValue";
        String value = "toDelete";

        IKVMessage putResponse = kvClient.put(key, value);
        assertEquals(putResponse.getValue(), value);

        IKVMessage deleteResponse = kvClient.put(key, "null");
        assertEquals(deleteResponse.getStatus(), StatusType.DELETE_SUCCESS);
        assertEquals(deleteResponse.getKey(), key);
        assertNull(deleteResponse.getValue());
    }

    @Test
    public void testDeleteWithNullObject() throws Exception {
        String key = "deleteTestValue";
        String value = "toDelete";

        IKVMessage putResponse = kvClient.put(key, value);
        assertEquals(putResponse.getValue(), value);

        IKVMessage deleteResponse = kvClient.put(key, null);
        assertEquals(deleteResponse.getStatus(), StatusType.DELETE_SUCCESS);
        assertEquals(deleteResponse.getKey(), key);
        assertNull(deleteResponse.getValue());
    }

    @Test
    public void testDeleteNonExistentValueReturnsDeleteError() throws Exception {
        String key = "thisIsNotAValue";
        IKVMessage deleteResponse = kvClient.put(key, null);
        assertEquals(deleteResponse.getStatus(), StatusType.DELETE_ERROR);
        assertEquals(deleteResponse.getKey(), key);
        assertNull(deleteResponse.getValue());
    }

    @Test
    public void testDeleteKeyTwiceReturnsDeleteErrorOnSecondRequest() throws Exception {
        String key = "thisIsNotAValue";
        String value = "toTryDeleteTwice";

        kvClient.put(key, value);

        IKVMessage firstDeleteRequest = kvClient.put(key, null);
        assertEquals(firstDeleteRequest.getStatus(), StatusType.DELETE_SUCCESS);
        assertEquals(firstDeleteRequest.getKey(), key);
        assertNull(firstDeleteRequest.getValue());

        IKVMessage secondDeleteResponse = kvClient.put(key, null);
        assertEquals(secondDeleteResponse.getStatus(), StatusType.DELETE_ERROR);
        assertEquals(secondDeleteResponse.getKey(), key);
        assertNull(secondDeleteResponse.getValue());
    }

    @Test
    public void testGet() throws Exception {
        String key = "foo";
        String value = "bar";

        kvClient.put(key, value);
        IKVMessage response = kvClient.get(key);

        assertEquals(response.getStatus(), StatusType.GET_SUCCESS);
        assertEquals(response.getKey(), key);
        assertEquals(response.getValue(), value);
    }

    @Test
    public void testGetUnsetValue() throws Exception {
        String key = "an unset value";

        IKVMessage response = kvClient.get(key);

        assertEquals(response.getStatus(), StatusType.GET_ERROR);
        assertEquals(response.getKey(), key);
        assertNull(response.getValue());
    }

    @Test
    @Ignore
    public void testOneClientWithFiveConcurrentRequests() throws InterruptedException {
        final int NUMBER_OF_REQUESTS = 5;

        ExecutorService service = Executors.newFixedThreadPool(NUMBER_OF_REQUESTS);

        IntStream.range(0, NUMBER_OF_REQUESTS).forEach(i -> service.submit(() -> {
            try {
                String keyAndValue = String.valueOf(i);
                kvClient.put(keyAndValue, keyAndValue);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));

        service.shutdown();
        boolean putRequestsDidComplete = service.awaitTermination(30_000, TimeUnit.MILLISECONDS);
        assertTrue(putRequestsDidComplete);

        IntStream.range(0, NUMBER_OF_REQUESTS).forEach(i -> {
            try {
                String key = String.valueOf(i);
                IKVMessage getResponse = kvClient.get(key);
                assertEquals(key, getResponse.getValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testFiveConcurrentClientsWith25TotalConcurrentRequests() throws InterruptedException {
        final int NUMBER_OF_CLIENTS = 5;
        final int NUMBER_OF_REQUESTS = 25;

        final ArrayList<KVStore> clients = new ArrayList<>();

        try {
            IntStream.range(0, NUMBER_OF_CLIENTS).forEach(i -> clients.add(new KVStore("localhost", KV_SERVER_PORT)));

            assertEquals(clients.size(), NUMBER_OF_CLIENTS);

            clients.forEach(client -> {
                try {
                    client.connect();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            ExecutorService service = Executors.newFixedThreadPool(NUMBER_OF_CLIENTS);

            IntStream.range(0, NUMBER_OF_REQUESTS).forEach(i -> service.submit(() -> {
                try {
                    String keyAndValue = String.valueOf(i);
                    clients.get(i % NUMBER_OF_CLIENTS).put(keyAndValue, keyAndValue);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));

            service.shutdown();
            boolean putRequestsDidComplete = service.awaitTermination(60_000, TimeUnit.MILLISECONDS);
            assertTrue(putRequestsDidComplete);

            KVStore firstClient = clients.get(0);

            IntStream.range(0, NUMBER_OF_REQUESTS).forEach(i -> {
                try {
                    String key = String.valueOf(i);
                    IKVMessage getResponse = firstClient.get(key);
                    assertEquals(key, getResponse.getValue());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            clients.forEach(KVStore::disconnect);
        }
    }
}
