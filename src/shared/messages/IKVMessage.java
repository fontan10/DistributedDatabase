package shared.messages;

public interface IKVMessage {

    public enum StatusType {
        /**
         * KVStore <-> KVServer Messages
         **/
        GET,                    /* Get - request */
        GET_ERROR,              /* requested tuple (i.e. value) not found */
        GET_SUCCESS,            /* requested tuple (i.e. value) found */
        PUT,                    /* Put - request */
        PUT_SUCCESS,            /* Put - request successful, tuple inserted */
        PUT_UPDATE,             /* Put - request successful, i.e. value updated */
        PUT_ERROR,              /* Put - request not successful */
        DELETE_SUCCESS,         /* Delete - request successful */
        DELETE_ERROR,           /* Delete - request successful */
        FAILED,
        SERVER_STOPPED,         /* Server is stopped, no requests are processed */
        SERVER_WRITE_LOCK,      /* Server is locked for write, only get possible */
        SERVER_NOT_RESPONSIBLE, /* Request not successful, server not responsible for key */
        KEYRANGE,               /* Keyrange - request */
        KEYRANGE_SUCCESS,       /* Keyrange - request successful, list of ranges and the corresponding servers are returned as a list of semicolon separated triples */

        /**
         * KVServer <-> ECS Messages
         **/
        CONNECT,                /* Connect - request, KVServer requests to connect to ECS */
        CONNECT_SUCCESS,        /* Connect - request successful, KVServer can now activate and accept client requests */
        DISCONNECT,             /* Disconnect - request, KVServer requests to disconnect from ECS */
        DISCONNECT_SUCCESS,     /* Disconnect - request successful, KVServer can safely disconnect */
        TRANSFER,               /* Transfer - request, ECS requests KVServer to begin transfer of data */
        TRANSFER_SUCCESS,       /* Transfer - request successful, KVServer has completed transferring data */
        TRANSFER_END,           /* Transfer - KVServer can safely delete data */
        METADATA_UPDATE,        /* Metadata - KVServer must update metadata */
    }

    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    public String getKey();

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    public String getValue();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    public StatusType getStatus();
}


