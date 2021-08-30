package com.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;
import org.apache.commons.logging.Log;

public class StringUtils {
    private static final DecimalFormat decimalFormat;
    private static final BitSet PRINTABLE_CHARS = new BitSet(256);
    private static final byte PRINTABLE_ESCAPE_CHAR = 61;
    private static DecimalFormat oneDecimal;
    public static final char COMMA = ',';
    public static final String COMMA_STR = ",";
    public static final char ESCAPE_CHAR = '\\';

    public StringUtils() {
    }

    public static String stringifyException(Throwable e) {
        StringWriter stm = new StringWriter();
        PrintWriter wrt = new PrintWriter(stm);
        e.printStackTrace(wrt);
        wrt.close();
        return stm.toString();
    }

    public static String simpleHostname(String fullHostname) {
        int offset = fullHostname.indexOf(46);
        return offset != -1 ? fullHostname.substring(0, offset) : fullHostname;
    }

    public static String humanReadableInt(long number) {
        long absNumber = Math.abs(number);
        double result = (double)number;
        String suffix = "";
        if (absNumber < 1024L) {
            return String.valueOf(number);
        } else {
            if (absNumber < 1048576L) {
                result = (double)number / 1024.0D;
                suffix = "K";
            } else if (absNumber < 1073741824L) {
                result = (double)number / 1048576.0D;
                suffix = "M";
            } else {
                result = (double)number / 1.073741824E9D;
                suffix = "G";
            }

            return oneDecimal.format(result) + suffix;
        }
    }

    public static String formatPercent(double done, int digits) {
        DecimalFormat percentFormat = new DecimalFormat("0.00%");
        double scale = Math.pow(10.0D, (double)(digits + 2));
        double rounded = Math.floor(done * scale);
        percentFormat.setDecimalSeparatorAlwaysShown(false);
        percentFormat.setMinimumFractionDigits(digits);
        percentFormat.setMaximumFractionDigits(digits);
        return percentFormat.format(rounded / scale);
    }

    public static String arrayToString(String[] strs) {
        if (strs.length == 0) {
            return "";
        } else {
            StringBuffer sbuf = new StringBuffer();
            sbuf.append(strs[0]);

            for(int idx = 1; idx < strs.length; ++idx) {
                sbuf.append(",");
                sbuf.append(strs[idx]);
            }

            return sbuf.toString();
        }
    }

    public static String byteToHexString(byte[] bytes, int start, int end) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes == null");
        } else {
            StringBuilder s = new StringBuilder();

            for(int i = start; i < end; ++i) {
                s.append(String.format("%02x", bytes[i]));
            }

            return s.toString();
        }
    }

    public static String byteToHexString(byte[] bytes) {
        return byteToHexString(bytes, 0, bytes.length);
    }

    public static byte[] hexStringToByte(String hex) {
        byte[] bts = new byte[hex.length() / 2];

        for(int i = 0; i < bts.length; ++i) {
            bts[i] = (byte)Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }

        return bts;
    }

    public static String uriToString(URI[] uris) {
        if (uris == null) {
            return null;
        } else {
            StringBuffer ret = new StringBuffer(uris[0].toString());

            for(int i = 1; i < uris.length; ++i) {
                ret.append(",");
                ret.append(uris[i].toString());
            }

            return ret.toString();
        }
    }

    public static URI[] stringToURI(String[] str) {
        if (str == null) {
            return null;
        } else {
            URI[] uris = new URI[str.length];

            for(int i = 0; i < str.length; ++i) {
                try {
                    uris[i] = new URI(str[i]);
                } catch (URISyntaxException var4) {
                    System.out.println("Exception in specified URI's " + stringifyException(var4));
                    uris[i] = null;
                }
            }

            return uris;
        }
    }

    public static String formatTimeDiff(long finishTime, long startTime) {
        long timeDiff = finishTime - startTime;
        return formatTime(timeDiff);
    }

    public static String formatTime(long timeDiff) {
        StringBuffer buf = new StringBuffer();
        long hours = timeDiff / 3600000L;
        long rem = timeDiff % 3600000L;
        long minutes = rem / 60000L;
        rem %= 60000L;
        long seconds = rem / 1000L;
        if (hours != 0L) {
            buf.append(hours);
            buf.append("hrs, ");
        }

        if (minutes != 0L) {
            buf.append(minutes);
            buf.append("mins, ");
        }

        buf.append(seconds);
        buf.append("sec");
        return buf.toString();
    }

    public static String getFormattedTimeWithDiff(DateFormat dateFormat, long finishTime, long startTime) {
        StringBuffer buf = new StringBuffer();
        if (0L != finishTime) {
            buf.append(dateFormat.format(new Date(finishTime)));
            if (0L != startTime) {
                buf.append(" (" + formatTimeDiff(finishTime, startTime) + ")");
            }
        }

        return buf.toString();
    }

    public static String[] getStrings(String str) {
        Collection<String> values = getStringCollection(str);
        return values.size() == 0 ? null : (String[])values.toArray(new String[values.size()]);
    }

    public static Collection<String> getStringCollection(String str) {
        List<String> values = new ArrayList();
        if (str == null) {
            return values;
        } else {
            StringTokenizer tokenizer = new StringTokenizer(str, ",");
            values = new ArrayList();

            while(tokenizer.hasMoreTokens()) {
                values.add(tokenizer.nextToken());
            }

            return values;
        }
    }

    public static String[] split(String str) {
        return split(str, '\\', ',');
    }

    public static String[] split(String str, char escapeChar, char separator) {
        if (str == null) {
            return null;
        } else {
            ArrayList<String> strList = new ArrayList();
            StringBuilder split = new StringBuilder();
            int index = 0;

            while((index = findNext(str, separator, escapeChar, index, split)) >= 0) {
                ++index;
                strList.add(split.toString());
                split.setLength(0);
            }

            strList.add(split.toString());
            int last = strList.size();

            while(true) {
                --last;
                if (last < 0 || !"".equals(strList.get(last))) {
                    return (String[])strList.toArray(new String[strList.size()]);
                }

                strList.remove(last);
            }
        }
    }

    public static String[] split(String str, char separatorChar) {
        return splitWorker(str, separatorChar, false);
    }

    public static int findNext(String str, char separator, char escapeChar, int start, StringBuilder split) {
        int numPreEscapes = 0;

        for(int i = start; i < str.length(); ++i) {
            char curChar = str.charAt(i);
            if (numPreEscapes == 0 && curChar == separator) {
                return i;
            }

            split.append(curChar);
            int var10000;
            if (curChar == escapeChar) {
                ++numPreEscapes;
                var10000 = numPreEscapes % 2;
            } else {
                var10000 = 0;
            }

            numPreEscapes = var10000;
        }

        return -1;
    }

    public static String escapeString(String str) {
        return escapeString(str, '\\', ',');
    }

    public static String escapeString(String str, char escapeChar, char charToEscape) {
        return escapeString(str, escapeChar, new char[]{charToEscape});
    }

    private static boolean hasChar(char[] chars, char character) {
        char[] var2 = chars;
        int var3 = chars.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            char target = var2[var4];
            if (character == target) {
                return true;
            }
        }

        return false;
    }

    public static String escapeString(String str, char escapeChar, char[] charsToEscape) {
        if (str == null) {
            return null;
        } else {
            StringBuilder result = new StringBuilder();

            for(int i = 0; i < str.length(); ++i) {
                char curChar = str.charAt(i);
                if (curChar == escapeChar || hasChar(charsToEscape, curChar)) {
                    result.append(escapeChar);
                }

                result.append(curChar);
            }

            return result.toString();
        }
    }

    public static String unEscapeString(String str) {
        return unEscapeString(str, '\\', ',');
    }

    public static String unEscapeString(String str, char escapeChar, char charToEscape) {
        return unEscapeString(str, escapeChar, new char[]{charToEscape});
    }

    public static String unEscapeString(String str, char escapeChar, char[] charsToEscape) {
        if (str == null) {
            return null;
        } else {
            StringBuilder result = new StringBuilder(str.length());
            boolean hasPreEscape = false;

            for(int i = 0; i < str.length(); ++i) {
                char curChar = str.charAt(i);
                if (hasPreEscape) {
                    if (curChar != escapeChar && !hasChar(charsToEscape, curChar)) {
                        throw new IllegalArgumentException("Illegal escaped string " + str + " unescaped " + escapeChar + " at " + (i - 1));
                    }

                    result.append(curChar);
                    hasPreEscape = false;
                } else {
                    if (hasChar(charsToEscape, curChar)) {
                        throw new IllegalArgumentException("Illegal escaped string " + str + " unescaped " + curChar + " at " + i);
                    }

                    if (curChar == escapeChar) {
                        hasPreEscape = true;
                    } else {
                        result.append(curChar);
                    }
                }
            }

            if (hasPreEscape) {
                throw new IllegalArgumentException("Illegal escaped string " + str + ", not expecting " + escapeChar + " in the end.");
            } else {
                return result.toString();
            }
        }
    }

    public static String getHostname() {
        try {
            return "" + InetAddress.getLocalHost();
        } catch (UnknownHostException var1) {
            return "" + var1;
        }
    }

    private static String toStartupShutdownString(String prefix, String[] msg) {
        StringBuffer b = new StringBuffer(prefix);
        b.append("\n/************************************************************");
        String[] var3 = msg;
        int var4 = msg.length;

        for(int var5 = 0; var5 < var4; ++var5) {
            String s = var3[var5];
            b.append("\n" + prefix + s);
        }

        b.append("\n************************************************************/");
        return b.toString();
    }

    public static void startupShutdownMessage(Class<?> clazz, String[] args, final Log LOG) {
        final String hostname = getHostname();
        final String classname = clazz.getSimpleName();
        LOG.info(toStartupShutdownString("STARTUP_MSG: ", new String[]{"Starting " + classname, "  host = " + hostname, "  args = " + Arrays.asList(args)}));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOG.info(StringUtils.toStartupShutdownString("SHUTDOWN_MSG: ", new String[]{"Shutting down " + classname + " at " + hostname}));
            }
        });
    }

    public static String escapeHTML(String string) {
        if (string == null) {
            return null;
        } else {
            StringBuffer sb = new StringBuffer();
            boolean lastCharacterWasSpace = false;
            char[] chars = string.toCharArray();
            char[] var4 = chars;
            int var5 = chars.length;

            for(int var6 = 0; var6 < var5; ++var6) {
                char c = var4[var6];
                if (c == ' ') {
                    if (lastCharacterWasSpace) {
                        lastCharacterWasSpace = false;
                        sb.append("&nbsp;");
                    } else {
                        lastCharacterWasSpace = true;
                        sb.append(" ");
                    }
                } else {
                    lastCharacterWasSpace = false;
                    switch(c) {
                        case '"':
                            sb.append("&quot;");
                            break;
                        case '&':
                            sb.append("&amp;");
                            break;
                        case '<':
                            sb.append("&lt;");
                            break;
                        case '>':
                            sb.append("&gt;");
                            break;
                        default:
                            sb.append(c);
                    }
                }
            }

            return sb.toString();
        }
    }

    public static String byteDesc(long len) {
        double val = 0.0D;
        String ending = "";
        if (len < 1048576L) {
            val = 1.0D * (double)len / 1024.0D;
            ending = " KB";
        } else if (len < 1073741824L) {
            val = 1.0D * (double)len / 1048576.0D;
            ending = " MB";
        } else if (len < 1099511627776L) {
            val = 1.0D * (double)len / 1.073741824E9D;
            ending = " GB";
        } else if (len < 1125899906842624L) {
            val = 1.0D * (double)len / 1.099511627776E12D;
            ending = " TB";
        } else {
            val = 1.0D * (double)len / 1.125899906842624E15D;
            ending = " PB";
        }

        return limitDecimalTo2(val) + ending;
    }

    public static synchronized String limitDecimalTo2(double d) {
        return decimalFormat.format(d);
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    public static boolean isNullOrEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    public static boolean isBlank(String str) {
        int strLen;
        if (str != null && (strLen = str.length()) != 0) {
            for(int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(str.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static String join(Object[] array, char separator) {
        return array == null ? null : join(array, separator, 0, array.length);
    }

    public static String join(Object[] array, char separator, int startIndex, int endIndex) {
        if (array == null) {
            return null;
        } else {
            int bufSize = endIndex - startIndex;
            if (bufSize <= 0) {
                return "";
            } else {
                bufSize *= (array[startIndex] == null ? 16 : array[startIndex].toString().length()) + 1;
                StringBuffer buf = new StringBuffer(bufSize);

                for(int i = startIndex; i < endIndex; ++i) {
                    if (i > startIndex) {
                        buf.append(separator);
                    }

                    if (array[i] != null) {
                        buf.append(array[i]);
                    }
                }

                return buf.toString();
            }
        }
    }

    public static String join(Object[] array, String separator) {
        return array == null ? null : join(array, separator, 0, array.length);
    }

    public static String join(Object[] array, String separator, int startIndex, int endIndex) {
        if (array == null) {
            return null;
        } else {
            if (separator == null) {
                separator = "";
            }

            int bufSize = endIndex - startIndex;
            if (bufSize <= 0) {
                return "";
            } else {
                bufSize *= (array[startIndex] == null ? 16 : array[startIndex].toString().length()) + separator.length();
                StringBuilder buf = new StringBuilder(bufSize);

                for(int i = startIndex; i < endIndex; ++i) {
                    if (i > startIndex) {
                        buf.append(separator);
                    }

                    if (array[i] != null) {
                        buf.append(array[i]);
                    }
                }

                return buf.toString();
            }
        }
    }

    public static String[] splitPreserveAllTokens(String str, char separatorChar) {
        return splitWorker(str, separatorChar, true);
    }

    private static String[] splitWorker(String str, char separatorChar, boolean preserveAllTokens) {
        if (str == null) {
            return null;
        } else {
            int len = str.length();
            if (len == 0) {
                return new String[0];
            } else {
                List list = new ArrayList();
                int i = 0;
                int start = 0;
                boolean match = false;
                boolean lastMatch = false;

                while(true) {
                    while(i < len) {
                        if (str.charAt(i) == separatorChar) {
                            if (match || preserveAllTokens) {
                                list.add(str.substring(start, i));
                                match = false;
                                lastMatch = true;
                            }

                            ++i;
                            start = i;
                        } else {
                            lastMatch = false;
                            match = true;
                            ++i;
                        }
                    }

                    if (match || preserveAllTokens && lastMatch) {
                        list.add(str.substring(start, i));
                    }

                    return (String[])((String[])list.toArray(new String[list.size()]));
                }
            }
        }
    }

    public static String strip(String str, String stripChars) {
        if (isEmpty(str)) {
            return str;
        } else {
            str = stripStart(str, stripChars);
            return stripEnd(str, stripChars);
        }
    }

    public static String stripStart(String str, String stripChars) {
        int strLen;
        if (str != null && (strLen = str.length()) != 0) {
            int start = 0;
            if (stripChars == null) {
                while(start != strLen && Character.isWhitespace(str.charAt(start))) {
                    ++start;
                }
            } else {
                if (stripChars.length() == 0) {
                    return str;
                }

                while(start != strLen && stripChars.indexOf(str.charAt(start)) != -1) {
                    ++start;
                }
            }

            return str.substring(start);
        } else {
            return str;
        }
    }

    public static String stripEnd(String str, String stripChars) {
        int end;
        if (str != null && (end = str.length()) != 0) {
            if (stripChars == null) {
                while(end != 0 && Character.isWhitespace(str.charAt(end - 1))) {
                    --end;
                }
            } else {
                if (stripChars.length() == 0) {
                    return str;
                }

                while(end != 0 && stripChars.indexOf(str.charAt(end - 1)) != -1) {
                    --end;
                }
            }

            return str.substring(0, end);
        } else {
            return str;
        }
    }

    public static boolean equals(String str1, String str2) {
        return str1 == null ? str2 == null : str1.equals(str2);
    }

    public static final byte[] encodeQuotedPrintable(byte[] bytes) {
        if (bytes == null) {
            return null;
        } else {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] var2 = bytes;
            int var3 = bytes.length;

            for(int var4 = 0; var4 < var3; ++var4) {
                byte c = var2[var4];
                int b = c;
                if (c < 0) {
                    b = 256 + c;
                }

                if (PRINTABLE_CHARS.get(b)) {
                    buffer.write(b);
                } else {
                    buffer.write(61);
                    char hex1 = Character.toUpperCase(Character.forDigit(b >> 4 & 15, 16));
                    char hex2 = Character.toUpperCase(Character.forDigit(b & 15, 16));
                    buffer.write(hex1);
                    buffer.write(hex2);
                }
            }

            return buffer.toByteArray();
        }
    }

    static {
        NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ENGLISH);
        decimalFormat = (DecimalFormat)numberFormat;
        decimalFormat.applyPattern("#.##");

        int i;
        for(i = 33; i <= 60; ++i) {
            PRINTABLE_CHARS.set(i);
        }

        for(i = 62; i <= 123; ++i) {
            PRINTABLE_CHARS.set(i);
        }

        PRINTABLE_CHARS.set(125);
        PRINTABLE_CHARS.set(126);
        oneDecimal = new DecimalFormat("0.0");
    }

    public static enum TraditionalBinaryPrefix {
        KILO(1024L),
        MEGA(KILO.value << 10),
        GIGA(MEGA.value << 10),
        TERA(GIGA.value << 10),
        PETA(TERA.value << 10),
        EXA(PETA.value << 10);

        public final long value;
        public final char symbol;

        private TraditionalBinaryPrefix(long value) {
            this.value = value;
            this.symbol = this.toString().charAt(0);
        }

        public static StringUtils.TraditionalBinaryPrefix valueOf(char symbol) {
            symbol = Character.toUpperCase(symbol);
            StringUtils.TraditionalBinaryPrefix[] var1 = values();
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
                StringUtils.TraditionalBinaryPrefix prefix = var1[var3];
                if (symbol == prefix.symbol) {
                    return prefix;
                }
            }

            throw new IllegalArgumentException("Unknown symbol '" + symbol + "'");
        }

        public static long string2long(String s) {
            s = s.trim();
            int lastpos = s.length() - 1;
            char lastchar = s.charAt(lastpos);
            if (Character.isDigit(lastchar)) {
                return Long.parseLong(s);
            } else {
                long prefix = valueOf(lastchar).value;
                long num = Long.parseLong(s.substring(0, lastpos));
                if (num <= 9223372036854775807L / prefix && num >= -9223372036854775808L / prefix) {
                    return num * prefix;
                } else {
                    throw new IllegalArgumentException(s + " does not fit in a Long");
                }
            }
        }
    }
}

