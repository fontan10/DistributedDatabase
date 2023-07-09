package shared.messages;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.NoSuchElementException;
import java.util.Scanner;

import static shared.messages.KVMessage.SERIALIZATION_FOOTER;

public class SocketMessenger {
    private final Object lock = new Object();
    private final Scanner input;
    private final DataOutputStream output;

    public SocketMessenger(Socket socket) throws IOException {
        this.input = new Scanner(socket.getInputStream());
        this.input.useDelimiter(SERIALIZATION_FOOTER);

        this.output = new DataOutputStream(socket.getOutputStream());
    }

    public void sendMessage(KVMessage message) throws IOException {
        synchronized (lock) {
            byte[] byteMessage = KVMessage.writeToBytes(message);
            output.write(byteMessage);
            output.flush();
        }
    }

    public KVMessage receiveMessage() throws IOException {
        synchronized (lock) {
            try {
                // The Scanner will strip the message delimiter \r\n, thus we need to
                // re-append it to the message since we want KVMessage's readFromBytes
                // and writeToBytes to use a consistent serialization format.
                return KVMessage.readFromBytes((input.nextLine() + SERIALIZATION_FOOTER).getBytes());
            } catch (NoSuchElementException e) {
                // This is thrown when the scanner has reached EOF which will only happen if the Socket has closed
                if (input.ioException() != null) {
                    throw input.ioException();
                }

				throw new IOException("no message was received");
            }
        }
    }
}
