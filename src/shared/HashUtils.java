package shared;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.stream.IntStream;

public class HashUtils {
    public static final int MD5_EXPECTED_LENGTH = 32;
    public static final String MIN_MD5_HASH = "0".repeat(MD5_EXPECTED_LENGTH);
    public static final String MAX_MD5_HASH = "f".repeat(MD5_EXPECTED_LENGTH);

    private static final Logger LOGGER = Logger.getRootLogger();

    /**
     * Builds MD5 hash for value.
     *
     * @param value the string to be hashed using MD5
     * @return hexadecimal representation (length-32 string) of MD5 hash
     */
    public static String md5(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder stringBuilder = new StringBuilder();
            for (byte b : hashBytes) {
                stringBuilder.append(String.format("%02x", b));
            }
            return stringBuilder.toString();
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("Cannot get MD5 MessageDigest instance", e);
        }

        return null;
    }

    /**
     * Computes previous hash (hexadecimal value minus one) of a given MD5 hash.
     *
     * @param hash MD5 hash in hexadecimal (length-32 string)
     * @return MD5 hash minus one in hexadecimal (length-32 string)
     */
    public static String hashSubtractOne(String hash) {
        if (hash.length() != MD5_EXPECTED_LENGTH) {
            throw new IllegalArgumentException("Hash is not correct length for MD5");
        }

        if (hash.equals(MIN_MD5_HASH)) {
            return MAX_MD5_HASH;
        }

        String hashMinusOne = new BigInteger(hash, 16).subtract(new BigInteger("1")).toString(16);
        return "0".repeat(MD5_EXPECTED_LENGTH - hashMinusOne.length()) + hashMinusOne;
    }
}
