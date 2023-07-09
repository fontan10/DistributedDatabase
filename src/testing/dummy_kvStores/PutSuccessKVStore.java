package testing.dummy_kvStores;

import client.KVCommInterface;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

public class PutSuccessKVStore implements KVCommInterface {
    @Override
    public void connect() throws Exception {
    }

    @Override
    public void disconnect() {
    }

    @Override
    public IKVMessage put(String key, String value) throws Exception {
        if (value == null) {
            return new KVMessage(key, value, IKVMessage.StatusType.DELETE_SUCCESS);
        } else {
            return new KVMessage(key, value, IKVMessage.StatusType.PUT_SUCCESS);
        }
    }

    @Override
    public IKVMessage get(String key) throws Exception {
        return null;
    }
}
