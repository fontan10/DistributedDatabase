package testing;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;

public class KVMessageTest {
    @BeforeClass
    public static void beforeAll() {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLargeKeyInstantiationFails() {
        String key = "a".repeat(KVMessage.MAX_KEY_BYTE_LENGTH * 2);
        new KVMessage(key, IKVMessage.StatusType.GET);
    }

    @Test
    public void testMaxKeySizeInstantiation() {
        String key = "a".repeat(KVMessage.MAX_KEY_BYTE_LENGTH);
        new KVMessage(key, IKVMessage.StatusType.GET);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLargeValueInstantiationFails() {
        String value = "a".repeat(KVMessage.MAX_VAL_BYTE_LENGTH * 2);
        new KVMessage("testkey", value, IKVMessage.StatusType.PUT);
    }

    @Test
    public void testMaxValueSizeInstantiation() {
        String value = "a".repeat(KVMessage.MAX_VAL_BYTE_LENGTH);
        new KVMessage("testkey", value, IKVMessage.StatusType.PUT);
    }

    @Test
    public void testLargeKeyInstantiationWorksForFailedMessage() {
        String failure = "a".repeat(KVMessage.MAX_KEY_BYTE_LENGTH * 5);
        new KVMessage(failure, IKVMessage.StatusType.FAILED);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailedMessageWithNonNullValueFails() {
        String failure = "a".repeat(KVMessage.MAX_KEY_BYTE_LENGTH * 5);
        new KVMessage(failure, "b", IKVMessage.StatusType.FAILED);
    }
}
