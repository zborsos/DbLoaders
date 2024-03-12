package local.utils;

import org.eclipse.emf.ecore.util.EcoreUtil;

import java.io.Serializable;

/**
 * Represents a Universally Unique Identifier ( <a
 * href="http://www.ietf.org/rfc/rfc4122.txt">UUID </a>).
 *
 * UUIDs are used to uniquely identify the identity and state of each element in
 * a ITeamRepository.
 *
 *
 * @since 0.5
 * @NoImplement This class is not intended to be subclassed by clients.
 */
public final class uid implements Comparable, Serializable {
    private static final long serialVersionUID = 1548313159849892768L;

    // Ordered list of possible values of the last character in the UUID.
    private static char[] LASTCHAR_DIGITS = "AQgw".toCharArray(); //$NON-NLS-1$

    // Ordered list of all possible values in a UUID.
    private static final char[] BASE64_DIGITS = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz".toCharArray(); //$NON-NLS-1$

    // getUUIDValue() returns strings prefixed with the following:
    private static final char UUID_STARTS_WITH = '_';

    private static final int UUID_CHAR_LENGTH = 23;

    private static final char MAX_UUID_CHAR = 128;

    private static final int[] DIGIT_MAP;

    private static final int[] LASTCHAR_DIGIT_MAP;

    //private static final Log LOG = LogFactory.getLog(UUID.class.getName());

    static {
        DIGIT_MAP = computeInverseMap(BASE64_DIGITS);
        LASTCHAR_DIGIT_MAP = computeInverseMap(LASTCHAR_DIGITS);
    }


    /**
     * Create a new UUID.
     *
     * Note: UUIDs are created based upon manipulating a time seed, which in the
     * end is only as precise as Java on the underlying operating system.
     * Batched requests which create > 1000 UUID's serially may eventually cause
     * pausing, and potentially failure should the number of immediate request
     * be larger than what the underlying clock granularity supports.
     *
     * @return a new Universally Unique Identifier.
     * @throws Error
     *             when an UUID cannot be generated due to clock synchronization
     *             issues.
     * @ShortOp This is a short operation; it may block only momentarily; safe
     *          to call from a responsive thread.
     */
    public static uid generate() {
        return createFrom(EcoreUtil.generateUUID().toCharArray());
    }

    /**
     * Reconstitute a UUID from a String representation. This method will do
     * basic heuristic checking to see if it appears to be a valid
     * representation for our UUID. This method serves as a mechanism for a
     * client to rebuild a UUID object by value : it is expected that the value
     * was derived from a valid UUID instance at some point in time.
     *
     * @param uuidValue
     *            desired value for new UUID
     * @return UUID corresponding to specified value
     * @throws IllegalArgumentException
     *             for invalid uuidValue
     * @ShortOp This is a short operation; it may block only momentarily; safe
     *          to call from a responsive thread.
     */
    public static uid valueOf(String uuidValue) {
        // Catch illegal null uuidValue before we NPE trying to parse it
        if (uuidValue == null) {
            throw new IllegalArgumentException("null UUID");  //$NON-NLS-1$
        }
        char[] trimmedValue = uuidValue.trim().toCharArray();
        if (!validateUUID(trimmedValue)) {
            // We had to remove the invalid UUID from the exception message for
            // reasons.  In case someday we need to debug this error, we can
            // set a property here to dump the invalid UUID to stderr.
            boolean shouldLogInvalidUUID = Boolean.valueOf(System.getProperty("com.ibm.team.repository.common.UUID.dumpInvalidUUIDsToStderr", "false")); //$NON-NLS-1$ //$NON-NLS-2$
            if (shouldLogInvalidUUID == true)  {
                System.out.println("com.ibm.team.repository.common.UUID.valueOf(String) invalidUUID: ["+uuidValue+"]"); //$NON-NLS-1$ //$NON-NLS-2$
                //LOG.error("com.ibm.team.repository.common.UUID.valueOf(String) invalidUUID: ["+uuidValue+"]");  //$NON-NLS-1$//$NON-NLS-2$
            }
            throw new IllegalArgumentException(String.format("invalid UUID"));   //$NON-NLS-1$
        }
        return createFrom(trimmedValue);
    }

    private static uid createFrom(char[] trimmedValue) {
        long msb = 0;
        char next;
        long nextBits;

        // Character 0 is skipped (always 0).

        // Characters 1-10 are mapped onto the upper bits of the msb long
        for (int idx = 1; idx < 11; idx++) {
            next = trimmedValue[idx];

            nextBits = DIGIT_MAP[next];
            msb <<= 6;
            msb |= nextBits;
        }

        // Character 11 is spread across the msb and lsb
        next = trimmedValue[11];
        nextBits = DIGIT_MAP[next];
        msb <<= 4;
        msb |= nextBits >> 2;

        long lsb = nextBits & 0x0003l;

        // Characters 12-21 map onto bits 2-61 of the lsb, starting with the most significant bits
        for (int idx = 12; idx < 22; idx++) {
            next = trimmedValue[idx];

            nextBits = DIGIT_MAP[next];
            lsb <<= 6;
            lsb |= nextBits;
        }

        // Character 22 only has 4 possible values, which fill the remaining 2 bits of the lsb
        next = trimmedValue[22];
        nextBits = LASTCHAR_DIGIT_MAP[next];
        lsb <<= 2;
        lsb |= nextBits;

        return new uid(msb, lsb);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return (int) (msb ^ lsb ^ (msb >> 32) ^ (lsb >> 32));
    }

    /**
     * Gets the 64-bit hash value for this {@link uid}. This has much better collision resistance than <code>int {@link #hashCode()}</code>.
     *
     * @return the 64-bit hash value
     */
    public long longHashCode() {
        return (msb * 31L) + lsb;
    }

    /**
     * Get the String representation of a UUID. It encodes the 128 bit UUID in
     * <a href="http://www.ietf.org/rfc/rfc2045.txt">base 64 </a>, but rather
     * than padding the encoding with two "=" characters, it prefixes the
     * encoding with a single "_" character, to ensure that the result is a
     * valid <a href="http://www.w3.org/TR/xmlschema-2/#ID">ID </a>, i.e., an <a
     * href="http://www.w3.org/TR/1999/REC-xml-names-19990114/#NT-NCName">NCName
     * </a>
     *
     * Note : the string representation is a derived value and is not cached within
     * the UUID object.  Calling this method multiple times will return different
     * (but equivalent) String instances.
     *
     * @return representation of UUID
     * @ShortOp This is a short operation; it may block only momentarily; safe
     *          to call from a responsive thread.
     */
    public String getUuidValue() {
        char[] result = new char[UUID_CHAR_LENGTH];

        // Position 0 is always an underscore
        result[0] = UUID_STARTS_WITH;

        // Fill in position 22 from the least significant 2 bits
        long localLsb = this.lsb;
        result[22] = LASTCHAR_DIGITS[(int) (localLsb & 0x03l)];
        localLsb >>>= 2;

        // Fill in positions 12 through 21 with the next least significant bits
        for (int i = 21; i >= 12; i--) {
            result[i] = BASE64_DIGITS[(int)(localLsb & 0x3f)];
            localLsb >>>= 6;
        }

        long localMsb = this.msb;

        // Fill in position 11 with the 2 most significant bits from lsb and the 4 least significant
        // bits of msb
        result[11] = BASE64_DIGITS[(int) (localLsb | ((localMsb & 0x0f) << 2))];
        localMsb >>>= 4;

        for (int i = 10; i != 0; i--)
        {
            result[i] = BASE64_DIGITS[(int) (localMsb & 0x3fl)];
            localMsb >>>= 6;
        }

        return new String(result);
    }

    /**
     * Standard equality method for UUID. Two UUID's are considered to be equal
     * if and only if their string representations are equal.
     *
     * e.g. UUID.valueOf( uuid.getUuidValue() ).equals( uuid ) returns true.
     *
     * @param obj
     *            object to consider comparison against
     * @return <code>true</code> if obj is a UUID and has the same String
     *         representation as this UUID.
     * @see Object#equals(Object)
     * @ShortOp This is a short operation; it may block only momentarily; safe
     *          to call from a responsive thread.
     */
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof uid)) {
            return false;
        }
        uid uid = (uid)obj;
        return msb == uid.msb && lsb == uid.lsb;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(Object o) {
        if (o instanceof uid) {
            uid uid = (uid)o;
            int result = unsignedCompare(msb, uid.msb);
            if (result == 0) {
                return unsignedCompare(lsb, uid.lsb);
            }
            return result;
        }
        return 1;
    }

    /**
     * Treat the given longs as unsigned, and compare them
     * @param l1
     * @param l2
     * @return
     */
    private int unsignedCompare(long l1, long l2) {
        if (l1 == l2) {
            return 0;
        }
        long msb1 = l1 >>> 32;
        long msb2 = l2 >>> 32;

        if (msb1 == msb2) {
            long lsb1 = l1 & 0xffffffff;
            long lsb2 = l2 & 0xffffffff;

            return lsb1 < lsb2 ? -1 : 1;
        } else {
            return msb1 < msb2 ? -1 : 1;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return String.format("[UUID %s]", getUuidValue());  //$NON-NLS-1$
    }

    /**
     * Integer representation of this UUID.
     */
    private final long msb;
    private final long lsb;

    /**
     * Check validity of specified string as a UUID.
     *
     * Guess (as much as possible) whether the passed String is a valid UUID.
     * Basically, there are several ways we can eliminate a string as invalid,
     * but no way to prove it is valid.
     *
     * @param possibleUUID
     *            value to be analyzed
     *
     * @return <code>true</code> if the value could be a UUID, and
     *         <code>false</code> if the value could not possibly be a UUID.
     * @ShortOp This is a short operation; it may block only momentarily; safe
     *          to call from a responsive thread.
     */
    private static boolean validateUUID(char[] possibleUUID) {
        // Test the length of the UUID, we generate 23 character UUIDs
        int len = possibleUUID.length;
        if (len != UUID_CHAR_LENGTH) {
            return false;
        }
        char[] uuidChars = possibleUUID;
        // Our UUID's start with an underscore
        char c = uuidChars[0];
        if (c != UUID_STARTS_WITH) {
            return false;
        }
        // All of the characters in a UUID must be in the range of MIME64
        // encoding characters
        for (int i = 0; i < len; i++) {
            c = uuidChars[i];
            if (c > MAX_UUID_CHAR || DIGIT_MAP[c] == -1) {
                return false;
            }
        }
        return true;
    }

    private uid(long msb, long lsb) {
        this.msb = msb;
        this.lsb = lsb;
    }


    private static int[] computeInverseMap(char[] digits) {
        int[] digitMap = new int[MAX_UUID_CHAR + 1];

        for (int i = 0; i < digitMap.length; i++) {
            digitMap[i] = -1;
        }
        for (int i = 0; i < digits.length; i++) {
            char c = digits[i];
            if (c > MAX_UUID_CHAR) {
                throw new IllegalStateException();
            }
            digitMap[c] = i;
        }
        return digitMap;
    }
}
