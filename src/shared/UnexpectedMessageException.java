package shared;

import shared.messages.IKVMessage;

public class UnexpectedMessageException extends Exception {
	public UnexpectedMessageException(IKVMessage msg) {
		this(msg, null);
	}

	public UnexpectedMessageException(IKVMessage msg, String reason) {
		super("unexpected message of type " + msg.getStatus() +
				(reason == null ? "" : " " + reason));
	}
}
