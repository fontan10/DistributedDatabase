package testing;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class MessageSerializationTest {
    public static final String TEST_KEY = "key\nwithnewline";
    public static final String TEST_VALUE = "value with\rspace\nand\nspecialchars";

    @BeforeClass
    public static void beforeAll() {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteToBytesPut() {
        IKVMessage.StatusType[] types = new IKVMessage.StatusType[]{
                IKVMessage.StatusType.PUT,
                IKVMessage.StatusType.PUT_SUCCESS,
                IKVMessage.StatusType.PUT_UPDATE,
                IKVMessage.StatusType.PUT_ERROR,
        };
        for (IKVMessage.StatusType type : types) {
            KVMessage message = new KVMessage(TEST_KEY, TEST_VALUE, type);
            byte[] serialized = KVMessage.writeToBytes(message);

            String expected = type.name().toLowerCase() + " " + TEST_KEY + " " + TEST_VALUE + "\r\n";
            assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), serialized);
        }
    }

    @Test
    public void testWriteToBytesPutNull() {
        IKVMessage.StatusType[] types = new IKVMessage.StatusType[]{
                IKVMessage.StatusType.PUT,
                // PUT_SUCCESS should not ever have value = null
                IKVMessage.StatusType.PUT_UPDATE,
                IKVMessage.StatusType.PUT_ERROR,
        };
        for (IKVMessage.StatusType type : types) {
            KVMessage message = new KVMessage(TEST_KEY, type);
            byte[] serialized = KVMessage.writeToBytes(message);

            String expected = type.name().toLowerCase() + " " + TEST_KEY + "\r\n";
            assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), serialized);
        }
    }

    @Test
    public void testWriteToBytesGet() {
        IKVMessage.StatusType[] types = new IKVMessage.StatusType[]{
                IKVMessage.StatusType.GET,
                IKVMessage.StatusType.GET_ERROR,
        };
        for (IKVMessage.StatusType type : types) {
            KVMessage message = new KVMessage(TEST_KEY, type);
            byte[] serialized = KVMessage.writeToBytes(message);

            String expected = type.name().toLowerCase() + " " + TEST_KEY + "\r\n";
            assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), serialized);
        }

        KVMessage message = new KVMessage(TEST_KEY, TEST_VALUE, IKVMessage.StatusType.GET_SUCCESS);
        byte[] serialized = KVMessage.writeToBytes(message);
        String expected = "get_success " + TEST_KEY + " " + TEST_VALUE + "\r\n";
        assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), serialized);
    }

    @Test
    public void testReadFromBytesPut() {
        IKVMessage.StatusType[] types = new IKVMessage.StatusType[]{
                IKVMessage.StatusType.PUT,
                IKVMessage.StatusType.PUT_SUCCESS,
                IKVMessage.StatusType.PUT_UPDATE,
                IKVMessage.StatusType.PUT_ERROR,
        };
        for (IKVMessage.StatusType type : types) {
            String serialized = type.name().toLowerCase() + " " + TEST_KEY + " " + TEST_VALUE + "\r\n";
            KVMessage parsed = KVMessage.readFromBytes(serialized.getBytes(StandardCharsets.UTF_8));

            assertEquals(type, parsed.getStatus());
            assertEquals(TEST_KEY, parsed.getKey());
            assertEquals(TEST_VALUE, parsed.getValue());
        }
    }

    @Test
    public void testReadFromBytesDelete() {
        IKVMessage.StatusType[] types = new IKVMessage.StatusType[]{
                IKVMessage.StatusType.DELETE_SUCCESS,
                IKVMessage.StatusType.DELETE_ERROR,
        };
        for (IKVMessage.StatusType type : types) {
            String serialized = type.name().toLowerCase() + " " + TEST_KEY + "\r\n";
            KVMessage parsed = KVMessage.readFromBytes(serialized.getBytes(StandardCharsets.UTF_8));

            assertEquals(type, parsed.getStatus());
            assertEquals(TEST_KEY, parsed.getKey());
            assertNull(parsed.getValue());
        }
    }

    @Test
    public void testReadFromBytesPutNull() {
        IKVMessage.StatusType[] types = new IKVMessage.StatusType[]{
                IKVMessage.StatusType.PUT,
                // PUT_SUCCESS and PUT_UPDATE should not ever have value = null
                IKVMessage.StatusType.PUT_ERROR,
        };
        for (IKVMessage.StatusType type : types) {
            String serialized = type.name().toLowerCase() + " " + TEST_KEY + "\r\n";
            KVMessage parsed = KVMessage.readFromBytes(serialized.getBytes(StandardCharsets.UTF_8));

            assertEquals(type, parsed.getStatus());
            assertEquals(TEST_KEY, parsed.getKey());
            assertNull(parsed.getValue());
        }
    }

    @Test
    public void testReadFromBytesPutNullString() {
        IKVMessage.StatusType[] types = new IKVMessage.StatusType[]{
                IKVMessage.StatusType.PUT,
                // PUT_SUCCESS and PUT_UPDATE should not ever have value = null
                IKVMessage.StatusType.PUT_ERROR,
        };
        for (IKVMessage.StatusType type : types) {
            String serialized = type.name().toLowerCase() + " " + TEST_KEY + " null\r\n";
            KVMessage parsed = KVMessage.readFromBytes(serialized.getBytes(StandardCharsets.UTF_8));

            assertEquals(type, parsed.getStatus());
            assertEquals(TEST_KEY, parsed.getKey());
            assertNull(parsed.getValue());
        }
    }

    @Test
    public void testReadFromBytesGet() {
        IKVMessage.StatusType[] types = new IKVMessage.StatusType[]{
                IKVMessage.StatusType.GET,
                IKVMessage.StatusType.GET_ERROR,
        };
        for (IKVMessage.StatusType type : types) {
            String serialized = type.name().toLowerCase() + " " + TEST_KEY + "\r\n";
            KVMessage parsed = KVMessage.readFromBytes(serialized.getBytes(StandardCharsets.UTF_8));

            assertEquals(type, parsed.getStatus());
            assertEquals(TEST_KEY, parsed.getKey());
            assertNull(parsed.getValue());
        }

        String serialized = "get_success " + TEST_KEY + " " + TEST_VALUE + "\r\n";
        KVMessage parsed = KVMessage.readFromBytes(serialized.getBytes(StandardCharsets.UTF_8));
        assertEquals(IKVMessage.StatusType.GET_SUCCESS, parsed.getStatus());
        assertEquals(TEST_KEY, parsed.getKey());
        assertEquals(TEST_VALUE, parsed.getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadFromBytesThrowsWithInvalidEnding() {
        String invalid = "get asdf";
        KVMessage.readFromBytes(invalid.getBytes(StandardCharsets.UTF_8));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadFromBytesThrowsWithNoKey() {
        String invalid = "get\r\n";
        KVMessage.readFromBytes(invalid.getBytes(StandardCharsets.UTF_8));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadFromBytesThrowsWithInvalidStatusType() {
        String invalid = "get_NOTReally key\r\n";
        KVMessage.readFromBytes(invalid.getBytes(StandardCharsets.UTF_8));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadFromBytesThrowsWhenValueIsNotPresent() {
        String invalid = "get_success key\r\n";
        KVMessage.readFromBytes(invalid.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testWriteToAndReadFromBytesPut() {
        KVMessage original = new KVMessage(TEST_KEY, TEST_VALUE, IKVMessage.StatusType.PUT);
        byte[] bytes = KVMessage.writeToBytes(original);
        KVMessage output = KVMessage.readFromBytes(bytes);

        assertEquals(original.getStatus(), output.getStatus());
        assertEquals(original.getKey(), output.getKey());
        assertEquals(original.getValue(), output.getValue());
    }

    @Test
    public void testWriteToAndReadFromBytesGet() {
        KVMessage original = new KVMessage(TEST_KEY, KVMessage.StatusType.GET);
        byte[] bytes = KVMessage.writeToBytes(original);
        KVMessage output = KVMessage.readFromBytes(bytes);

        assertEquals(original.getStatus(), output.getStatus());
        assertEquals(original.getKey(), output.getKey());
        assertEquals(original.getValue(), output.getValue());
    }

    @Test
    public void testWriteToAndReadFromBytesServerStopped() {
        KVMessage original = new KVMessage(IKVMessage.StatusType.SERVER_STOPPED);
        byte[] bytes = KVMessage.writeToBytes(original);
        KVMessage output = KVMessage.readFromBytes(bytes);

        assertEquals(original.getStatus(), output.getStatus());
        assertEquals(original.getKey(), output.getKey());
        assertEquals(original.getValue(), output.getValue());
    }
}
