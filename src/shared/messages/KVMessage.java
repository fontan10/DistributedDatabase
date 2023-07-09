package shared.messages;

import java.nio.charset.StandardCharsets;

import static shared.messages.IKVMessage.StatusType.*;

public class KVMessage implements IKVMessage {
    public static final String SERIALIZATION_FOOTER = "\r\n";
    public static final int MAX_KEY_BYTE_LENGTH = 20;
    public static final int MAX_VAL_BYTE_LENGTH = 122_880; // 120 kBytes

    private final String key;
    private final String value;
    private final StatusType statusType;

    public KVMessage(String key, String value, StatusType statusType) {
        this.key = key;
        this.value = value;
        this.statusType = statusType;

        // In a FAILED message, the key is an error description which does not have a maximum length
        if (statusType == FAILED) {
            if (value != null) {
                throw new IllegalArgumentException("FAILED messages cannot contain a value");
            }
            return;
        }

        if (this.key != null && this.key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTE_LENGTH) {
            throw new IllegalArgumentException("key cannot be larger than " + MAX_KEY_BYTE_LENGTH + " bytes");
        }
        if (this.value != null && this.value.getBytes(StandardCharsets.UTF_8).length > MAX_VAL_BYTE_LENGTH) {
            throw new IllegalArgumentException("value cannot be larger than " + MAX_VAL_BYTE_LENGTH + " bytes");
        }
    }

    public KVMessage(String key, StatusType statusType) {
        this(key, null, statusType);
    }

    public KVMessage(StatusType statusType) {
        this(null, null, statusType);
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return statusType;
    }

    public static byte[] writeToBytes(KVMessage message) {
        StringBuilder builder = new StringBuilder();
        builder.append(message.statusType.name().toLowerCase());
        if (message.key != null) {
            builder.append(" ");
            builder.append(message.key);
        }
        if (message.value != null) {
            builder.append(" ");
            builder.append(message.value);
        }
        builder.append(SERIALIZATION_FOOTER);
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static KVMessage readFromBytes(byte[] bytes) throws IllegalArgumentException {
        String payload = new String(bytes, StandardCharsets.UTF_8);

        if (!payload.endsWith(SERIALIZATION_FOOTER)) {
            throw new IllegalArgumentException("the string should end with \\r\\n");
        }

        int payloadAdjustedLength = payload.length() - 2; // -2 adjustment for \r\n
        int statusIdx = payload.contains(" ") ? payload.indexOf(" ") : payloadAdjustedLength;

        StatusType statusType = StatusType.valueOf(payload.substring(0, statusIdx).toUpperCase());
        switch (statusType) {
            case PUT:
            case PUT_SUCCESS:
            case PUT_UPDATE:
            case PUT_ERROR:
            case GET_SUCCESS:
            case CONNECT:
            case TRANSFER:
            case TRANSFER_SUCCESS: {
                boolean valueCanContainNull = statusType == PUT || statusType == PUT_ERROR;
                int keyIdx = payload.indexOf(" ", statusIdx + 1);
                if (keyIdx == -1) {
                    // If there is no " " following the status, then the value is null
                    if (!valueCanContainNull) {
                        throw new IllegalArgumentException(statusType.name() + " should have space-delimited key and value");
                    }

                    String key = payload.substring(statusIdx + 1, payloadAdjustedLength);
                    return new KVMessage(key, statusType);
                }

                String key = payload.substring(statusIdx + 1, keyIdx);
                String value = payload.substring(keyIdx + 1, payloadAdjustedLength);
                if (valueCanContainNull && value.equals("null")) {
                    value = null;
                }
                return new KVMessage(key, value, statusType);
            }
            case GET:
            case GET_ERROR:
            case DELETE_ERROR:
            case DELETE_SUCCESS:
            case KEYRANGE_SUCCESS:
            case FAILED:
            case TRANSFER_END:
            case METADATA_UPDATE: {
                if (statusIdx == payloadAdjustedLength) {
                    throw new IllegalArgumentException(statusType.name() + " should have space-delimited status type and key");
                }
                String key = payload.substring(statusIdx + 1, payloadAdjustedLength);
                return new KVMessage(key, statusType);
            }
            case SERVER_STOPPED:
            case SERVER_WRITE_LOCK:
            case SERVER_NOT_RESPONSIBLE:
            case KEYRANGE:
            case CONNECT_SUCCESS:
            case DISCONNECT:
            case DISCONNECT_SUCCESS:
                return new KVMessage(statusType);
            default: {
                throw new IllegalArgumentException("unsupported StatusType " + statusType.name());
            }
        }
    }
}
