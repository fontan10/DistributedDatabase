package testing;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;
import shared.HashUtils;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static shared.HashUtils.*;

public class HashUtilsTest {
    @BeforeClass
    public static void beforeAll() {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHashString() {
        String actualHash = HashUtils.md5("abc");
        assertEquals(MD5_EXPECTED_LENGTH, actualHash.length());
        assertEquals("900150983cd24fb0d6963f7d28e17f72", actualHash);
    }

    @Test
    public void testHashStringWhenResultingHashHasLeadingZeros() {
        // https://stackoverflow.com/a/25341591
        String actualHash = HashUtils.md5("jk8ssl");
        assertEquals(MD5_EXPECTED_LENGTH, actualHash.length());
        assertEquals("0000000018e6137ac2caab16074784a6", actualHash);
    }

    @Test
    public void testHashEmptyString() {
        String actualHash = HashUtils.md5("");
        assertEquals(MD5_EXPECTED_LENGTH, actualHash.length());
        assertEquals("d41d8cd98f00b204e9800998ecf8427e", actualHash);
    }
    
    @Test
    public void testPreviousHashOfTwelveReturnsEleven() {
        String givenHash = "0".repeat(31) + "c"; // 12
        String expectedHashMinusOne = "0".repeat(31) + "b"; // 11

        String actualPreviousHash = HashUtils.hashSubtractOne(givenHash);
        assertEquals(MD5_EXPECTED_LENGTH, actualPreviousHash.length());
        assertEquals(expectedHashMinusOne, actualPreviousHash);
    }

    @Test
    public void testPreviousHashReturnsHashMinusOne() {
        String startHash = "d41d8cd98f00b204e9800998ecf8427";
        String endHash = "e";
        String endHashMinusOne = "d";
        String givenHash = startHash + endHash;
        String expectedHashMinusOne = startHash + endHashMinusOne;

        String actualPreviousHash = HashUtils.hashSubtractOne(givenHash);
        assertEquals(MD5_EXPECTED_LENGTH, actualPreviousHash.length());
        assertEquals(expectedHashMinusOne, actualPreviousHash);
    }

    @Test
    public void testPreviousHashReturnsHashMinusOneGivenUpperCaseHash() {
        String startHash = "d41d8cd98f00b204e9800998ecf8427";
        String endHash = "e";
        String endHashMinusOne = "d";
        String givenHash = (startHash + endHash).toUpperCase();
        String expectedHashMinusOne = startHash + endHashMinusOne;

        String actualPreviousHash = HashUtils.hashSubtractOne(givenHash);
        assertEquals(MD5_EXPECTED_LENGTH, actualPreviousHash.length());
        assertEquals(expectedHashMinusOne, actualPreviousHash);
    }

    @Test
    public void testPreviousHashReturnsHashMinusOneGivenMixedUpperAndLowerCaseHash() {
        String startHash = "d41d8cd98f00b204";
        String middleHash = "e9800998ecf8427";
        String endHash = "e";
        String endHashMinusOne = "d";
        String givenHash = startHash + middleHash.toUpperCase() + endHash;
        String expectedHashMinusOne = startHash + middleHash + endHashMinusOne;

        String actualPreviousHash = HashUtils.hashSubtractOne(givenHash);
        assertEquals(MD5_EXPECTED_LENGTH, actualPreviousHash.length());
        assertEquals(expectedHashMinusOne, actualPreviousHash);
    }

    @Test
    public void testPreviousHashWithCarryingReturnsHashMinusOne() {
        String startHash = "123456789123456789123456789f0";
        String endHash = "a00";
        String endHashMinusOne = "9ff";
        String givenHash = startHash + endHash;
        String expectedHashMinusOne = startHash + endHashMinusOne;

        String actualPreviousHash = HashUtils.hashSubtractOne(givenHash);
        assertEquals(MD5_EXPECTED_LENGTH, actualPreviousHash.length());
        assertEquals(expectedHashMinusOne, actualPreviousHash);
    }

    @Test
    public void testPreviousHashOfMaxValueReturnsMaxValueMinusOne() {
        String expectedHashMinusOne = "f".repeat(31) + "e";

        String actualPreviousHash = HashUtils.hashSubtractOne(MAX_MD5_HASH);
        assertEquals(MD5_EXPECTED_LENGTH, actualPreviousHash.length());
        assertEquals(expectedHashMinusOne, actualPreviousHash);
    }

    @Test
    public void testPreviousHashOfZeroReturnsMaxValue() {
        String actualPreviousHash = HashUtils.hashSubtractOne(MIN_MD5_HASH);
        assertEquals(MD5_EXPECTED_LENGTH, actualPreviousHash.length());
        assertEquals(MAX_MD5_HASH, actualPreviousHash);
    }

    @Test
    public void testPreviousHashOfOneReturnsZero() {
        String givenHash = "0".repeat(31) + "1";

        String actualPreviousHash = HashUtils.hashSubtractOne(givenHash);
        assertEquals(MD5_EXPECTED_LENGTH, actualPreviousHash.length());
        assertEquals(MIN_MD5_HASH, actualPreviousHash);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPreviousHashThrowsGivenHashOfIncorrectLength() {
        HashUtils.hashSubtractOne("abc");
    }
}
