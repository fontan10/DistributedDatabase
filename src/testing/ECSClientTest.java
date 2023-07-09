package testing;

import app_kvECS.ECSClient;

import ecs.ECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.*;

import java.io.IOException;
import java.net.InetAddress;

import static org.junit.Assert.*;

public class ECSClientTest {
    private static ECSClient ecsClient;

    @BeforeClass
    public static void beforeAll() {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
            ecsClient = new ECSClient(InetAddress.getByName("localhost"), 6000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void beforeEach() {
        ecsClient.clearNodes();
    }

    @Test
    public void testAddNodeReturnsEntireHashRangeWhenAddingOneNodeToEmptyRing() throws IOException {
        String expectedNodeAddress = "localhost:7000"; // hashes to 13e0e6d7567a2fc9a167633e7bf6366e
        String expectedHashStart = "13e0e6d7567a2fc9a167633e7bf6366e";
        String expectedHashEnd = "13e0e6d7567a2fc9a167633e7bf6366d"; // start - 1

        String expected = expectedHashStart + "," + expectedHashEnd + "," + expectedNodeAddress;
        String actual = ecsClient.addNode("localhost", 7000);
        assertEquals(expected, actual);
    }

    @Test
    public void testAddNodeReturnsPartialHashRangeWhenNodeToRingWithNodes() throws IOException {
        ecsClient.addNode("localhost", 7000); // hashes to 13e0e6d7567a2fc9a167633e7bf6366e
        ecsClient.addNode("localhost", 8888); // hashes to 4cc54362930cb26261be0e29e6ad4a77
        ecsClient.addNode("localhost", 9000); // hashes to 838bcdaf3ed53ea3f32c338861088ddb

        String expected = "edb2af1bf318e60ed47d857bd6542391"
                + "," + "13e0e6d7567a2fc9a167633e7bf6366d" // hash of localhost:7000 - 1
                + "," + "localhost:10002";
        String actual = ecsClient.addNode("localhost", 10002); // hashes to edb2af1bf318e60ed47d857bd6542391
        assertEquals(expected, actual);
    }

    @Test
    public void testBuildMetadataReturnsEmptyStringGivenEmptyRing() {
        assertEquals("", ecsClient.buildMetadata());
    }

    @Test
    public void testBuildMetadataReturnsOneTripleGivenOneNode() throws IOException {
        ecsClient.addNode("localhost", 7000);
        String expectedNodeAddress = "localhost:7000"; // hashes to 13e0e6d7567a2fc9a167633e7bf6366e
        String expectedHashStart = "13e0e6d7567a2fc9a167633e7bf6366e";
        String expectedHashEnd = "13e0e6d7567a2fc9a167633e7bf6366d"; // start - 1

        String expected = expectedHashStart + "," + expectedHashEnd + "," + expectedNodeAddress + ";";
        String actual = ecsClient.buildMetadata();
        assertEquals(expected, actual);
    }

    @Test
    public void testBuildMetadataReturnsFourTriplesGivenFourNodes() throws IOException {
        ecsClient.addNode("localhost", 7000); // hashes to 13e0e6d7567a2fc9a167633e7bf6366e
        ecsClient.addNode("localhost", 8080); // hashes to 9f5ffc7a10e0bad054458b089947ce2f
        ecsClient.addNode("localhost", 9000); // hashes to 838bcdaf3ed53ea3f32c338861088ddb
        ecsClient.addNode("localhost", 10002); // hashes to edb2af1bf318e60ed47d857bd6542391

        String expected = "13e0e6d7567a2fc9a167633e7bf6366e" + "," // hash of localhost:7000
                + "838bcdaf3ed53ea3f32c338861088dda" + "," // hash of localhost:9000 - 1
                + "localhost:7000" + ";"
                + "838bcdaf3ed53ea3f32c338861088ddb" + "," // hash of localhost:9000
                + "9f5ffc7a10e0bad054458b089947ce2e" + "," // hash of localhost:8080 - 1
                + "localhost:9000" + ";"
                + "9f5ffc7a10e0bad054458b089947ce2f" + "," // hash of localhost:8080
                + "edb2af1bf318e60ed47d857bd6542390" + "," // hash of localhost:10002 - 1
                + "localhost:8080" + ";"
                + "edb2af1bf318e60ed47d857bd6542391" + "," // hash of localhost:10002
                + "13e0e6d7567a2fc9a167633e7bf6366d" + "," // hash of localhost:7000 - 1 (wrap around)
                + "localhost:10002" + ";";
        String actual = ecsClient.buildMetadata();
        assertEquals(expected, actual);
    }
}

