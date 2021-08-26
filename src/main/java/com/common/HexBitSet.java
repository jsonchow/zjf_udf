package com.common;

import com.sun.xml.internal.ws.util.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *     16进制字符串，模仿二进制位测试。字符仅能是16进制，忽略大小写，如果不是16进制字符，按0计算。
 *     如果指定位置没有设置值，按0计算（这个特性可以用于忽略前导0）
 *     顺序是从字符串右边向左边计算，最右边的字符串为0位。
 *     可重用。
 * </pre>
 */
public class HexBitSet {
    static int[] TEST = {0x1, 0x2, 0x4, 0x8};
    static int[] TEST_FROM = {0xf, 0xe, 0xc, 0x8};
    static int[] TEST_TO = {0x1, 0x3, 0x7, 0xf};
    static int[] TEST_COUNT = {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4};

    public final static char[] digits = {
            '0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'a', 'b',
            'c', 'd', 'e', 'f', 'g', 'h',
            'i', 'j', 'k', 'l', 'm', 'n',
            'o', 'p', 'q', 'r', 's', 't',
            'u', 'v', 'w', 'x', 'y', 'z'
    };

//    public final static BiFunction<Character, Character, Character> HEX_CHAR_AND = (x, y) -> Character.forDigit(Character.digit(x, 16) & Character.digit(y, 16), 16);
//    public final static BiFunction<Character, Character, Character> HEX_CHAR_OR = (x, y) -> Character.forDigit(Character.digit(x, 16) | Character.digit(y, 16), 16);
//
//    static public int calTrueCount(String hex, int from, int to) {
//        int len = hex.length() << 2;
//        if (from >= len) {
//            return 0;
//        }
//        int fromIdx = from >> 2;
//        if (to >= len) {
//            to = len - 1;
//        }
//        int toIdx = to >> 2;
//        int count = 0;
//        for (int i = fromIdx; i <= toIdx; i++) {
//            char c = hex.charAt(hex.length() - i - 1);
//            int n = Character.digit(c, 16);
//            if (i == fromIdx) {
//                int fromOffset = from % 4;
//                n = (n & TEST_FROM[fromOffset]);
//            }
//            if (i == toIdx) {
//                int toOffset = to % 4;
//                n = (n & TEST_TO[toOffset]);
//            }
//            if (n > 0) {
//                count += TEST_COUNT[n];
//            }
//        }
//        return count;
//
//    }
//
//    private static final int[] DIGIT_NUMBER = {
//            0, 1, 2, 3, 4, 5, 6, 7, 8, 9
//            , 0, 0, 0, 0, 0, 0
//    };
//
//    private static final int[] DIGIT_HEX = {
//            0, 10, 11, 12, 13, 14, 15, 0, 0, 0
//            , 0, 0, 0, 0, 0, 0
//    };
//
//    public static int digit16(char codePoint) {
//        if ((codePoint & 0x30) == 0x30) {
//            return DIGIT_NUMBER[codePoint & 0x0f];
//        }
//
//        if ((codePoint & 0x40) == 0x40 || (codePoint & 0x60) == 0x60) {
//            return DIGIT_HEX[codePoint & 0x0f];
//        }
//        return 0;
//    }
//
//    /**
//     * 探测给定位是否是1
//     */
//    static public boolean checkTrue(String hex, int bit) {
//        int len = hex.length() << 2;
//        if (bit < 0 || bit >= len) {
//            return false;
//        }
//        int idx = bit >> 2;
//        int offset = bit % 4;
//        char c = hex.charAt(hex.length() - idx - 1);
//        int n = Character.digit(c, 16);
//        return (n & TEST[offset]) == TEST[offset];
//    }
//
//    /**
//     * 探测给定位区间是否包含有1，0<=from<=to.测试位包含to
//     */
//    static public boolean checkTrue(String hex, int from, int to) {
//        if (from == to) {
//            return checkTrue(hex, from);
//        }
//        int len = hex.length() << 2;
//        if (from >= len) {
//            return false;
//        }
//
//        int fromIdx = from >> 2;
//        if (to >= len) {
//            len = len - 1;
//        }
//        int toIdx = to >> 2;
//        for (int i = fromIdx; i <= toIdx; i++) {
//            char c = hex.charAt(hex.length() - i - 1);
//            int n = Character.digit(c, 16);
//            if (i == fromIdx) {
//                int fromOffset = from % 4;
//                n = (n & TEST_FROM[fromOffset]);
//            }
//            if (i == toIdx) {
//                int toOffset = to % 4;
//                n = (n & TEST_TO[toOffset]);
//            }
//            if (n > 0) {
//                return true;
//            }
//
//        }
//        return false;
//    }
//
//
//    private String hex;
//
//    public String getHex() {
//        return hex;
//    }
//
//    public void setHex(String hex) {
//        this.hex = hex;
//    }
//
//    public int testTrueCount(int from, int to) {
//        return calTrueCount(hex, from, to);
//    }
//
//    public static String operator(BiFunction<Character, Character, Character> function, List<String> hexs) {
//        if (hexs == null || hexs.isEmpty()) {
//            return null;
//        }
//        return operator(function, hexs.stream().toArray(String[]::new));
//    }
//
//    public static String operator(BiFunction<Character, Character, Character> function, String... hexs) {
//        if (hexs == null || hexs.length == 0) {
//            return null;
//        }
//        if (hexs.length == 1) {
//            return hexs[0];
//        }
//
//        return IntStream.range(0, hexs.length)
//                .mapToObj(i -> hexs[i]).filter(StringUtils::isBlank).reduce((x, y) -> x.length() != y.length() ? x : IntStream.range(0, x.length()).map(j -> function.apply(x.charAt(j), y.charAt(j))).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString()).orElse(null);
//    }
//
//    static String initActiveHex() {
//        char[] chars = new char[30];
//        Arrays.fill(chars, '0');
//        return String.valueOf(chars);
//    }
//
//    public static String binToHexString(String binString) {
//        return IntStream.range(0, binString.length() / 4).mapToObj(i -> {
//            Long val = Long.parseUnsignedLong(binString.substring(i * 4, (i + 1) * 4), 2);
//            char[] buf = new char[1];
//            formatUnsiginedLong(val, 4, buf, 0, 1);
//            return String.valueOf(buf);
//        }).collect(Collectors.joining());
//    }
//
//
//    static int formatUnsiginedLong(long val, int shift, char[] buf, int offset, int len) {
//        int charPos = len;
//        int radix = 1 << shift;
//        int mask = radix - 1;
//        do {
//            buf[offset + --charPos] = HexBitSet.digits[((int) val) & mask];
//            val >>>= shift;
//        } while (charPos > 0);
//
//        return charPos;
//    }
//
//    public static void main(String[] args) {
//        HexBitSet bitset = new HexBitSet();
//        bitset.setHex("34c0420205953c02de68f864464d01");
//        System.out.println(bitset.testTrueCount(119,119));
//        System.out.println(bitset.testTrueCount(1,7));
//    }

}