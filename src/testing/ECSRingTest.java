package testing;

import ecs.ECSNode;
import ecs.ECSRing;
import org.junit.*;

import static org.junit.Assert.*;

import java.io.IOException;

public class ECSRingTest {
    private static ECSRing ring;

    @BeforeClass
    public static void beforeAll() {
        ring = new ECSRing();
    }

    @Before
    public void beforeEach() {
        ring.clearNodes();
    }

    @Test
    public void testGetNodeByKeyReturnsNullWhenRingIsEmpty() {
        assertNull(ring.getNodeForKey("key"));
    }

    @Test
    public void testGetNodeByKeyReturnsNodeGivenExactKeyOfNode() throws IOException {
        String exactKeyOfNode = "localhost:5000";
        ring.addNode("localhost", 5000);

        ECSNode node = ring.getNodeForKey(exactKeyOfNode);
        assertNotNull(node);
        assertEquals(exactKeyOfNode, node.getNodeIpAndPort());
    }

    @Test
    public void testGetNodeByKeyReturnsNodeGivenExactKeyOfNodeAndMultipleNodes() throws IOException {
        String exactKeyOfNode = "localhost:5000";
        ring.addNode("localhost", 5000);
        ring.addNode("localhost", 5001);
        ring.addNode("localhost", 5002);

        ECSNode node = ring.getNodeForKey(exactKeyOfNode);
        assertNotNull(node);
        assertEquals(exactKeyOfNode, node.getNodeIpAndPort());
    }

    @Test
    public void testGetNodeByKeyReturnsNodeGivenKeyThatHashesToValueGreaterThanNodeHashGivenRingWithOneNode() throws IOException {
        String expectedNodeAddress = "localhost:5000";
        ring.addNode("localhost", 5000); // localhost:5000 hashes to b18c9873dcbbe400e116c6e3d9644375
        String key = "abcd"; // hashes to e2fc714c4727ee9395f324cd2e7f331f (e > b)

        ECSNode node = ring.getNodeForKey(key);
        assertEquals(expectedNodeAddress, node.getNodeIpAndPort());
    }

    @Test
    public void testGetNodeByKeyReturnsNodeGivenKeyThatHashesToValueLessThanNodeHashGivenRingWithOneNode() throws IOException {
        String expectedNodeAddress = "localhost:5000";
        ring.addNode("localhost", 5000); // localhost:5000 hashes to b18c9873dcbbe400e116c6e3d9644375
        String key = "abc"; // hashes to 900150983cd24fb0d6963f7d28e17f72  (9 < b)

        ECSNode node = ring.getNodeForKey(key);
        assertEquals(expectedNodeAddress, node.getNodeIpAndPort());
    }

    @Test
    public void testGetNodeByKeyReturnsFlooredNodeGivenRingWithMultipleNodes() throws IOException {
        ring.addNode("localhost", 7000); // hashes to 13e0e6d7567a2fc9a167633e7bf6366e
        ring.addNode("localhost", 8080); // hashes to 9f5ffc7a10e0bad054458b089947ce2f
        ring.addNode("localhost", 9000); // hashes to 838bcdaf3ed53ea3f32c338861088ddb
        ring.addNode("localhost", 10002); // hashes to edb2af1bf318e60ed47d857bd6542391

        String expectedNodeAddress = "localhost:7000";

        String key = "ben"; // hashes to 13e0e6d7567a2fc9a167633e7bf6366e

        ECSNode node = ring.getNodeForKey(key);
        assertEquals(expectedNodeAddress, node.getNodeIpAndPort());
    }

    @Test
    public void testGetNodeByKeyReturnsFlooredNodeGivenRingWithMultipleNodesWithWrapAround() throws IOException {
        ring.addNode("localhost", 7000); // hashes to 13e0e6d7567a2fc9a167633e7bf6366e
        ring.addNode("localhost", 8080); // hashes to 9f5ffc7a10e0bad054458b089947ce2f
        ring.addNode("localhost", 9000); // hashes to 838bcdaf3ed53ea3f32c338861088ddb
        ring.addNode("localhost", 10002); // hashes to edb2af1bf318e60ed47d857bd6542391

        String expectedNodeAddress = "localhost:10002";

        String key = "jon"; // hashes to 006cb570acdab0e0bfc8e3dcb7bb4edf

        ECSNode node = ring.getNodeForKey(key);
        assertEquals(expectedNodeAddress, node.getNodeIpAndPort());
    }

    @Test
    public void testAddNodeReturnsCorrectHash() throws IOException {
        String expected = "13e0e6d7567a2fc9a167633e7bf6366e";
        String actual = ring.addNode("localhost", 7000);
        assertEquals(expected, actual);
    }
}
