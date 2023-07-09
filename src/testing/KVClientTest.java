package testing;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.*;
import app_kvClient.KVClient;
import testing.dummy_kvStores.PutSuccessKVStore;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class KVClientTest {
    PipedOutputStream terminalWriter;
    PipedInputStream terminalInput;
    ByteArrayOutputStream terminalOutput;

    static String logPath;

    final String UNKNOWN_COMMAND_MSG = "KVClient> Error! Unknown command";
    final String HELP_MESSAGE_MSG = "KVClient> KV CLIENT HELP (Usage):";
    final String INVALID_NUM_PARAMS_MSG = "KVClient> Error! Invalid number of parameters!";
    final String NOT_CONNECTED_TO_SERVER_MSG = "KVClient> Error! Please connect to an address and port";
    final String APPLICATION_EXIT_MSG = "KVClient> Application exit!";

    public void runAndQuit(KVClient kvClient) throws IOException {
        terminalWriter.write("quit\r\n".getBytes());
        kvClient.run();
    }

    public static String readFileAsString(String fileName) throws Exception {
        return new String(Files.readAllBytes(Paths.get(fileName)));
    }

    @BeforeClass
    public static void classSetUp() {
        String now = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
        logPath = "logs/testing/KVClientTest_" + now + ".log";
    }

    @Before
    public void setUp() throws IOException {
        new LogSetup(logPath, Level.ALL, true);

        terminalWriter = new PipedOutputStream();
        terminalInput = new PipedInputStream(terminalWriter);
        System.setIn(terminalInput);

        terminalOutput = new java.io.ByteArrayOutputStream();
        System.setOut(new java.io.PrintStream(terminalOutput));
    }

    @After
    public void cleanUp() {
        terminalOutput.reset();
        System.setOut(System.out);
    }

    @Test
    public void unknownCommand() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("UNKNOWN_COMMAND\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains(UNKNOWN_COMMAND_MSG));
        Assert.assertTrue(terminalOutput.toString().contains(HELP_MESSAGE_MSG));
    }

    @Test
    public void onlyWhitespaceCmd() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("                  \r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains(UNKNOWN_COMMAND_MSG));
        Assert.assertTrue(terminalOutput.toString().contains(HELP_MESSAGE_MSG));
    }

    @Test
    public void connectIncorrectNumArgs() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("connect\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains(INVALID_NUM_PARAMS_MSG));
    }

    @Test
    public void connectIncorrectNumArgs2() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("connect    \r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains(INVALID_NUM_PARAMS_MSG));
    }

    @Test
    public void connectIncorrectNumArgs3() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("connect hostname\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains(INVALID_NUM_PARAMS_MSG));
    }

    @Test
    public void connectIncorrectPort() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("connect hostname stringInsteadOfInt\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains("KVClient> Error! No valid address. Port must be a number!"));
    }

    // commands after disconnecting from Server
    @Test
    public void commandWithoutServer() throws IOException {
        KVClient kvClient = new KVClient();

        terminalWriter.write("put KEY val\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains(NOT_CONNECTED_TO_SERVER_MSG));
    }

    @Test
    public void commandWithoutServer2() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("disconnect\r\n".getBytes());
        terminalWriter.write("put KEY val\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains(NOT_CONNECTED_TO_SERVER_MSG));
    }

    @Test
    public void disconnectWhileNotConnected() throws IOException {
        KVClient kvClient = new KVClient();

        terminalWriter.write("disconnect\r\n".getBytes());

        runAndQuit(kvClient);

        // confirm no crash
    }

    @Test
    public void putIncorrectNumArgs() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("put\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains(INVALID_NUM_PARAMS_MSG));
    }

    @Test
    public void putSuccess() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("put      KEY VAL\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains("KVClient> Inserted <KEY> <VAL>"));
    }

    @Test
    public void putSuccess2() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("put KEY VAL VAL VAL\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains("KVClient> Inserted <KEY> <VAL VAL VAL>"));
    }

    @Test
    public void putSuccess3() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("put KEY VAL VAL VAL     \r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains("KVClient> Inserted <KEY> <VAL VAL VAL     >"));
    }

    @Test
    public void deleteSuccess() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("put KEY\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains("KVClient> Deleted <KEY>"));
    }

    @Test
    public void deleteSuccess2() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("put KEY        \r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains("KVClient> Deleted <KEY>"));
    }

    @Test
    public void putMaxByteCheck() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("put 012345678901234567890 val\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains("KVClient> Error! <012345678901234567890> is more than max length of 20 Bytes"));
    }

    @Test
    public void putMaxByteCheck2() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("put 012345678901234567890\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains("KVClient> Error! <012345678901234567890> is more than max length of 20 Bytes"));
    }

    @Test
    public void checkLogLevel() throws Exception {
        String checkLogLevelLogPath = logPath.substring(0, logPath.length()-4) + "_checkLogLevel.log";
        new LogSetup(checkLogLevelLogPath, Level.ALL, true);

        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("logLevel OFF\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertEquals(0, readFileAsString(checkLogLevelLogPath).length());
    }

    @Test
    public void checkLogLevel2() throws Exception {
        String checkLogLevelLogPath = logPath.substring(0, logPath.length()-4) + "_checkLogLevel2.log";
        new LogSetup(checkLogLevelLogPath, Level.ALL, true);

        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("logLevel ALL\r\n".getBytes());
        terminalWriter.write("put KEY VAL\r\n".getBytes());
        terminalWriter.write("logLevel OFF\r\n".getBytes());
        terminalWriter.write("put KEY2 VAL2\r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(readFileAsString(checkLogLevelLogPath).contains("INFO  [main] root: Inserted <KEY> <VAL>"));
        Assert.assertFalse(readFileAsString(checkLogLevelLogPath).contains("INFO  [main] root: Inserted <KEY2> <VAL2>"));
    }

    @Test
    public void helpMsg() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        terminalWriter.write("        help   \r\n".getBytes());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains(HELP_MESSAGE_MSG));
    }

    @Test
    public void quitMsg() throws IOException {
        KVClient kvClient = new KVClient(new PutSuccessKVStore());

        runAndQuit(kvClient);

        Assert.assertTrue(terminalOutput.toString().contains(APPLICATION_EXIT_MSG));
    }
}
