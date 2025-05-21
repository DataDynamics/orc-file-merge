package io.datadynamics.ibk.orc.common;

import java.io.IOException;
import java.util.*;

public class StringUtils {

    private static final String[] EMPTY_STRING_ARRAY = {};

    private static final String FOLDER_SEPARATOR = "/";

    private static final String WINDOWS_FOLDER_SEPARATOR = "\\";

    private static final String TOP_PATH = "..";

    private static final String CURRENT_PATH = ".";

    private static final char EXTENSION_SEPARATOR = '.';


    public static final String EMPTY_STRING = "";

    public static final byte[] EMPTY_BYTES = new byte[0];

    public static final String NEWLINE = SystemPropertyUtils.get("line.separator", "\n");

    public static final char DOUBLE_QUOTE = '\"';
    public static final char COMMA = ',';
    public static final char LINE_FEED = '\n';
    public static final char CARRIAGE_RETURN = '\r';
    public static final char TAB = '\t';
    public static final char SPACE = 0x20;

    private static final String[] BYTE2HEX_PAD = new String[256];
    private static final String[] BYTE2HEX_NOPAD = new String[256];
    private static final byte[] HEX2B;

    /**
     * 2 - Quote character at beginning and end.
     * 5 - Extra allowance for anticipated escape characters that may be added.
     */
    private static final int CSV_NUMBER_ESCAPE_CHARACTERS = 2 + 5;
    private static final char PACKAGE_SEPARATOR_CHAR = '.';

    static {
        // Generate the lookup table that converts a byte into a 2-digit hexadecimal integer.
        for (int i = 0; i < BYTE2HEX_PAD.length; i++) {
            String str = Integer.toHexString(i);
            BYTE2HEX_PAD[i] = i > 0xf ? str : ('0' + str);
            BYTE2HEX_NOPAD[i] = str;
        }
        // Generate the lookup table that converts an hex char into its decimal value:
        // the size of the table is such that the JVM is capable of save any bounds-check
        // if a char type is used as an index.
        HEX2B = new byte[Character.MAX_VALUE + 1];
        Arrays.fill(HEX2B, (byte) -1);
        HEX2B['0'] = 0;
        HEX2B['1'] = 1;
        HEX2B['2'] = 2;
        HEX2B['3'] = 3;
        HEX2B['4'] = 4;
        HEX2B['5'] = 5;
        HEX2B['6'] = 6;
        HEX2B['7'] = 7;
        HEX2B['8'] = 8;
        HEX2B['9'] = 9;
        HEX2B['A'] = 10;
        HEX2B['B'] = 11;
        HEX2B['C'] = 12;
        HEX2B['D'] = 13;
        HEX2B['E'] = 14;
        HEX2B['F'] = 15;
        HEX2B['a'] = 10;
        HEX2B['b'] = 11;
        HEX2B['c'] = 12;
        HEX2B['d'] = 13;
        HEX2B['e'] = 14;
        HEX2B['f'] = 15;
    }

    public static boolean isBlank(String string) {
        if (isEmpty(string)) {
            return true;
        }
        for (int i = 0; i < string.length(); i++) {
            if (!Character.isWhitespace(string.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isNotBlank(String string) {
        return !isBlank(string);
    }

    public static boolean isEmpty(String string) {
        return string == null || string.isEmpty();
    }

    public static boolean isNotEmpty(String string) {
        return !isEmpty(string);
    }

    public static String truncate(String string, int maxLength) {
        if (string.length() > maxLength) {
            return string.substring(0, maxLength);
        }
        return string;
    }

    public static String truncate(String string, int maxLength, String suffix) {
        return truncate(string, maxLength) + suffix;
    }


    /**
     * @link http://www.webreference.com/js/tips/000126.html
     */
    public static String convertTabSpaceSpecialCharacters(String message) {
        if (isEmpty(message)) {
            return message;
        }

        if ("\\t".equals(message)) { // Tab
            return replace(message, "\\t", "\u0009");
        } else if ("\\s".equals(message)) { // Space
            return replace(message, "\\s", "\u0020");
        }
        return message;
    }

    /**
     * Replace all occurences of a substring within a string with another string.
     *
     * @param inString   String to examine
     * @param oldPattern String to replace
     * @param newPattern String to insert
     * @return a String with the replacements
     */
    public static String replace(String inString, String oldPattern, String newPattern) {
        if (!hasLength(inString) || !hasLength(oldPattern) || newPattern == null) {
            return inString;
        }
        StringBuffer sbuf = new StringBuffer();
        // output StringBuffer we'll build up
        int pos = 0; // our position in the old string
        int index = inString.indexOf(oldPattern);
        // the index of an occurrence we've found, or -1
        int patLen = oldPattern.length();
        while (index >= 0) {
            sbuf.append(inString.substring(pos, index));
            sbuf.append(newPattern);
            pos = index + patLen;
            index = inString.indexOf(oldPattern, pos);
        }
        sbuf.append(inString.substring(pos));
        // remember to append any characters to the right of a match
        return sbuf.toString();
    }

    /**
     * Check that the given CharSequence is neither <code>null</code> nor of length 0.
     * Note: Will return <code>true</code> for a CharSequence that purely consists of whitespace.
     * <p><pre>
     * StringUtils.hasLength(null) = false
     * StringUtils.hasLength("") = false
     * StringUtils.hasLength(" ") = true
     * StringUtils.hasLength("Hello") = true
     * </pre>
     *
     * @param str the CharSequence to check (may be <code>null</code>)
     * @return <code>true</code> if the CharSequence is not null and has length
     */
    public static boolean hasLength(CharSequence str) {
        return (str != null && str.length() > 0);
    }


    /**
     * Check that the given String is neither <code>null</code> nor of length 0.
     * Note: Will return <code>true</code> for a String that purely consists of whitespace.
     *
     * @param str the String to check (may be <code>null</code>)
     * @return <code>true</code> if the String is not null and has length
     * @see #hasLength(CharSequence)
     */
    public static boolean hasLength(String str) {
        return hasLength((CharSequence) str);
    }

    /**
     * Performs the logic for the <code>split</code> and
     * <code>splitPreserveAllTokens</code> methods that return a maximum array
     * length.
     *
     * @param str               the String to parse, may be <code>null</code>
     * @param separatorChars    the separate character
     * @param max               the maximum number of elements to include in the
     *                          array. A zero or negative value implies no limit.
     * @param preserveAllTokens if <code>true</code>, adjacent separators are
     *                          treated as empty token separators; if <code>false</code>, adjacent
     *                          separators are treated as one separator.
     * @return an array of parsed Strings, <code>null</code> if null String input
     */
    private static String[] splitWorker(String str, String separatorChars, int max, boolean preserveAllTokens) {
        // Performance tuned for 2.0 (JDK1.4)
        // Direct code is quicker than StringTokenizer.
        // Also, StringTokenizer uses isSpace() not isWhitespace()

        if (str == null) {
            return null;
        }
        int len = str.length();
        if (len == 0) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }
        List list = new ArrayList();
        int sizePlus1 = 1;
        int i = 0, start = 0;
        boolean match = false;
        boolean lastMatch = false;
        if (separatorChars == null) {
            // Null separator means use whitespace
            while (i < len) {
                if (Character.isWhitespace(str.charAt(i))) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        } else if (separatorChars.length() == 1) {
            // Optimise 1 character case
            char sep = separatorChars.charAt(0);
            while (i < len) {
                if (str.charAt(i) == sep) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        } else {
            // standard case
            while (i < len) {
                if (separatorChars.indexOf(str.charAt(i)) >= 0) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        if ("".equals(str.substring(start, i)) == false)
                            list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        }
        if (match || (preserveAllTokens && lastMatch)) {
            list.add(str.substring(start, i));
        }
        return (String[]) list.toArray(new String[list.size()]);
    }

    /**
     * <p>Splits the provided text into an array with a maximum length,
     * separators specified, preserving all tokens, including empty tokens
     * created by adjacent separators.</p>
     * <p/>
     * <p>The separator is not included in the returned String array.
     * Adjacent separators are treated as separators for empty tokens.
     * Adjacent separators are treated as one separator.</p>
     * <p/>
     * <p>A <code>null</code> input String returns <code>null</code>.
     * A <code>null</code> separatorChars splits on whitespace.</p>
     * <p/>
     * <p>If more than <code>max</code> delimited substrings are found, the last
     * returned string includes all characters after the first <code>max - 1</code>
     * returned strings (including separator characters).</p>
     * <p/>
     * <pre>
     * StringUtils.splitPreserveAllTokens(null, *, *)            = null
     * StringUtils.splitPreserveAllTokens("", *, *)              = []
     * StringUtils.splitPreserveAllTokens("ab de fg", null, 0)   = ["ab", "cd", "ef"]
     * StringUtils.splitPreserveAllTokens("ab   de fg", null, 0) = ["ab", "cd", "ef"]
     * StringUtils.splitPreserveAllTokens("ab:cd:ef", ":", 0)    = ["ab", "cd", "ef"]
     * StringUtils.splitPreserveAllTokens("ab:cd:ef", ":", 2)    = ["ab", "cd:ef"]
     * StringUtils.splitPreserveAllTokens("ab   de fg", null, 2) = ["ab", "  de fg"]
     * StringUtils.splitPreserveAllTokens("ab   de fg", null, 3) = ["ab", "", " de fg"]
     * StringUtils.splitPreserveAllTokens("ab   de fg", null, 4) = ["ab", "", "", "de fg"]
     * </pre>
     *
     * @param str            the String to parse, may be <code>null</code>
     * @param separatorChars the characters used as the delimiters,
     *                       <code>null</code> splits on whitespace
     * @param max            the maximum number of elements to include in the
     *                       array. A zero or negative value implies no limit
     * @return an array of parsed Strings, <code>null</code> if null String input
     * @since 2.1
     */
    public static String[] splitPreserveAllTokens(String str, String separatorChars, int max) {
        return splitWorker(str, separatorChars, max, true);
    }

    /**
     * <p>Splits the provided text into an array, separators specified,
     * preserving all tokens, including empty tokens created by adjacent
     * separators. This is an alternative to using StringTokenizer.</p>
     * <p/>
     * <p>The separator is not included in the returned String array.
     * Adjacent separators are treated as separators for empty tokens.
     * For more control over the split use the StrTokenizer class.</p>
     * <p/>
     * <p>A <code>null</code> input String returns <code>null</code>.
     * A <code>null</code> separatorChars splits on whitespace.</p>
     * <p/>
     * <pre>
     * StringUtils.splitPreserveAllTokens(null, *)           = null
     * StringUtils.splitPreserveAllTokens("", *)             = []
     * StringUtils.splitPreserveAllTokens("abc def", null)   = ["abc", "def"]
     * StringUtils.splitPreserveAllTokens("abc def", " ")    = ["abc", "def"]
     * StringUtils.splitPreserveAllTokens("abc  def", " ")   = ["abc", "", def"]
     * StringUtils.splitPreserveAllTokens("ab:cd:ef", ":")   = ["ab", "cd", "ef"]
     * StringUtils.splitPreserveAllTokens("ab:cd:ef:", ":")  = ["ab", "cd", "ef", ""]
     * StringUtils.splitPreserveAllTokens("ab:cd:ef::", ":") = ["ab", "cd", "ef", "", ""]
     * StringUtils.splitPreserveAllTokens("ab::cd:ef", ":")  = ["ab", "", cd", "ef"]
     * StringUtils.splitPreserveAllTokens(":cd:ef", ":")     = ["", cd", "ef"]
     * StringUtils.splitPreserveAllTokens("::cd:ef", ":")    = ["", "", cd", "ef"]
     * StringUtils.splitPreserveAllTokens(":cd:ef:", ":")    = ["", cd", "ef", ""]
     * </pre>
     *
     * @param str            the String to parse, may be <code>null</code>
     * @param separatorChars the characters used as the delimiters,
     *                       <code>null</code> splits on whitespace
     * @return an array of parsed Strings, <code>null</code> if null String input
     * @since 2.1
     */
    public static String[] splitPreserveAllTokens(String str, String separatorChars) {
        return splitWorker(str, separatorChars, -1, true);
    }

    /**
     * Take a String which is a delimited list and convert it to a String array.
     * <p>A single delimiter can consists of more than one character: It will still
     * be considered as single delimiter string, rather than as bunch of potential
     * delimiter characters - in contrast to <code>tokenizeToStringArray</code>.
     *
     * @param str       the input String
     * @param delimiter the delimiter between elements (this is a single delimiter,
     *                  rather than a bunch individual delimiter characters)
     * @return an array of the tokens in the list
     */
    public static String[] delimitedListToStringArray(String str, String delimiter) {
        return delimitedListToStringArray(str, delimiter, null);
    }

    /**
     * Take a String which is a delimited list and convert it to a String array.
     * <p>A single delimiter can consists of more than one character: It will still
     * be considered as single delimiter string, rather than as bunch of potential
     * delimiter characters - in contrast to <code>tokenizeToStringArray</code>.
     *
     * @param str           the input String
     * @param delimiter     the delimiter between elements (this is a single delimiter,
     *                      rather than a bunch individual delimiter characters)
     * @param charsToDelete a set of characters to delete. Useful for deleting unwanted
     *                      line breaks: e.g. "\r\n\f" will delete all new lines and line feeds in a String.
     * @return an array of the tokens in the list
     */
    public static String[] delimitedListToStringArray(String str, String delimiter, String charsToDelete) {
        if (str == null) {
            return new String[0];
        }
        if (delimiter == null) {
            return new String[]{str};
        }
        List<String> result = new ArrayList<String>();
        if ("".equals(delimiter)) {
            for (int i = 0; i < str.length(); i++) {
                result.add(deleteAny(str.substring(i, i + 1), charsToDelete));
            }
        } else {
            int pos = 0;
            int delPos;
            while ((delPos = str.indexOf(delimiter, pos)) != -1) {
                result.add(deleteAny(str.substring(pos, delPos), charsToDelete));
                pos = delPos + delimiter.length();
            }
            if (str.length() > 0 && pos <= str.length()) {
                // 문자열의 나머지를 추가한다. 빈 문자열인 경우 추가하지 않는다.
                result.add(deleteAny(str.substring(pos), charsToDelete));
            }
        }
        return toStringArray(result);
    }

    /**
     * 지정한 문자열 콜렉션을 문자열 배열로 복사한다.
     * 콜렉션은 반드시 1개 이상의 엘리먼트가 포함되어야 하며 문자열이어야 한다.
     *
     * @param collection 복사할 콜렉션
     * @return 문자열 배열 (콜렉션이 <tt>null인 경우 <tt>null</tt>)
     */
    public static String[] toStringArray(Collection<String> collection) {
        if (collection == null) {
            return null;
        }
        return collection.toArray(new String[collection.size()]);
    }

    /**
     * 콤마가 구분자인 CSV 문자열을 문자열 배열로 변환한다.
     *
     * @param str 콤마를 구분자로 가진 CSV 문자열
     * @return 문자열 배열 또는 입력이 비어 있는 경우 빈 배열
     */
    public static String[] commaDelimitedListToStringArray(String str) {
        return delimitedListToStringArray(str, ",");
    }

    /**
     * 지정한 문자열에서 문자를 삭제한다.
     *
     * @param inString      원본 문자열
     * @param charsToDelete 삭제할 문자
     * @return 결과 문자열
     */
    public static String deleteAny(String inString, String charsToDelete) {
        if (!hasLength(inString) || !hasLength(charsToDelete)) {
            return inString;
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < inString.length(); i++) {
            char c = inString.charAt(i);
            if (charsToDelete.indexOf(c) == -1) {
                builder.append(c);
            }
        }
        return builder.toString();
    }

    public static boolean hasText(String str) {
        return (str != null && !str.isEmpty() && containsText(str));
    }

    private static boolean containsText(CharSequence str) {
        int strLen = str.length();
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public static boolean substringMatch(CharSequence str, int index, CharSequence substring) {
        if (index + substring.length() > str.length()) {
            return false;
        }
        for (int i = 0; i < substring.length(); i++) {
            if (str.charAt(index + i) != substring.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    public static int countOccurrencesOf(String str, String sub) {
        if (!hasLength(str) || !hasLength(sub)) {
            return 0;
        }

        int count = 0;
        int pos = 0;
        int idx;
        while ((idx = str.indexOf(sub, pos)) != -1) {
            ++count;
            pos = idx + sub.length();
        }
        return count;
    }

    public static String getFilename(String path) {
        if (path == null) {
            return null;
        }

        int separatorIndex = path.lastIndexOf(FOLDER_SEPARATOR);
        return (separatorIndex != -1 ? path.substring(separatorIndex + 1) : path);
    }

    public static String getFilenameExtension(String path) {
        if (path == null) {
            return null;
        }

        int extIndex = path.lastIndexOf(EXTENSION_SEPARATOR);
        if (extIndex == -1) {
            return null;
        }

        int folderIndex = path.lastIndexOf(FOLDER_SEPARATOR);
        if (folderIndex > extIndex) {
            return null;
        }

        return path.substring(extIndex + 1);
    }

    public static boolean containsWhitespace(CharSequence str) {
        if (!hasLength(str)) {
            return false;
        }

        int strLen = str.length();
        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsWhitespace(String str) {
        return containsWhitespace((CharSequence) str);
    }

    public static boolean startsWithIgnoreCase(String str, String prefix) {
        return (str != null && prefix != null && str.length() >= prefix.length() &&
                str.regionMatches(true, 0, prefix, 0, prefix.length()));
    }

    public static boolean endsWithIgnoreCase(String str, String suffix) {
        return (str != null && suffix != null && str.length() >= suffix.length() &&
                str.regionMatches(true, str.length() - suffix.length(), suffix, 0, suffix.length()));
    }

    public static String trimWhitespace(String str) {
        if (!hasLength(str)) {
            return str;
        }

        int beginIndex = 0;
        int endIndex = str.length() - 1;

        while (beginIndex <= endIndex && Character.isWhitespace(str.charAt(beginIndex))) {
            beginIndex++;
        }

        while (endIndex > beginIndex && Character.isWhitespace(str.charAt(endIndex))) {
            endIndex--;
        }

        return str.substring(beginIndex, endIndex + 1);
    }

    public static String delete(String inString, String pattern) {
        return replace(inString, pattern, "");
    }

    public static String quote(String str) {
        return (str != null ? "'" + str + "'" : null);
    }

    public static Object quoteIfString(Object obj) {
        return (obj instanceof String ? quote((String) obj) : obj);
    }

    public static String unqualify(String qualifiedName) {
        return unqualify(qualifiedName, '.');
    }

    public static String unqualify(String qualifiedName, char separator) {
        return qualifiedName.substring(qualifiedName.lastIndexOf(separator) + 1);
    }

    public static String capitalize(String str) {
        return changeFirstCharacterCase(str, true);
    }

    public static String uncapitalize(String str) {
        return changeFirstCharacterCase(str, false);
    }

    private static String changeFirstCharacterCase(String str, boolean capitalize) {
        if (!hasLength(str)) {
            return str;
        }

        char baseChar = str.charAt(0);
        char updatedChar;
        if (capitalize) {
            updatedChar = Character.toUpperCase(baseChar);
        } else {
            updatedChar = Character.toLowerCase(baseChar);
        }
        if (baseChar == updatedChar) {
            return str;
        }

        char[] chars = str.toCharArray();
        chars[0] = updatedChar;
        return new String(chars);
    }

    public static String[] tokenizeToStringArray(String str, String delimiters) {
        return tokenizeToStringArray(str, delimiters, true, true);
    }

    public static String[] tokenizeToStringArray(String str, String delimiters, boolean trimTokens, boolean ignoreEmptyTokens) {

        if (str == null) {
            return EMPTY_STRING_ARRAY;
        }

        StringTokenizer st = new StringTokenizer(str, delimiters);
        List<String> tokens = new ArrayList<>();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (trimTokens) {
                token = token.trim();
            }
            if (!ignoreEmptyTokens || token.length() > 0) {
                tokens.add(token);
            }
        }
        return toStringArray(tokens);
    }

    public static String cleanPath(String path) {
        if (!hasLength(path)) {
            return path;
        }

        String normalizedPath = replace(path, WINDOWS_FOLDER_SEPARATOR, FOLDER_SEPARATOR);
        String pathToUse = normalizedPath;

        // Shortcut if there is no work to do
        if (pathToUse.indexOf('.') == -1) {
            return pathToUse;
        }

        // Strip prefix from path to analyze, to not treat it as part of the
        // first path element. This is necessary to correctly parse paths like
        // "file:core/../core/io/Resource.class", where the ".." should just
        // strip the first "core" directory while keeping the "file:" prefix.
        int prefixIndex = pathToUse.indexOf(':');
        String prefix = "";
        if (prefixIndex != -1) {
            prefix = pathToUse.substring(0, prefixIndex + 1);
            if (prefix.contains(FOLDER_SEPARATOR)) {
                prefix = "";
            } else {
                pathToUse = pathToUse.substring(prefixIndex + 1);
            }
        }
        if (pathToUse.startsWith(FOLDER_SEPARATOR)) {
            prefix = prefix + FOLDER_SEPARATOR;
            pathToUse = pathToUse.substring(1);
        }

        String[] pathArray = delimitedListToStringArray(pathToUse, FOLDER_SEPARATOR);
        // we never require more elements than pathArray and in the common case the same number
        Deque<String> pathElements = new ArrayDeque<>(pathArray.length);
        int tops = 0;

        for (int i = pathArray.length - 1; i >= 0; i--) {
            String element = pathArray[i];
            if (CURRENT_PATH.equals(element)) {
                // Points to current directory - drop it.
            } else if (TOP_PATH.equals(element)) {
                // Registering top path found.
                tops++;
            } else {
                if (tops > 0) {
                    // Merging path element with element corresponding to top path.
                    tops--;
                } else {
                    // Normal path element found.
                    pathElements.addFirst(element);
                }
            }
        }

        // All path elements stayed the same - shortcut
        if (pathArray.length == pathElements.size()) {
            return normalizedPath;
        }
        // Remaining top paths need to be retained.
        for (int i = 0; i < tops; i++) {
            pathElements.addFirst(TOP_PATH);
        }
        // If nothing else left, at least explicitly point to current path.
        if (pathElements.size() == 1 && pathElements.getLast().isEmpty() && !prefix.endsWith(FOLDER_SEPARATOR)) {
            pathElements.addFirst(CURRENT_PATH);
        }

        final String joined = collectionToDelimitedString(pathElements, FOLDER_SEPARATOR);
        // avoid string concatenation with empty prefix
        return prefix.isEmpty() ? joined : prefix + joined;
    }

    public static String collectionToDelimitedString(Collection<?> coll, String delim) {
        return collectionToDelimitedString(coll, delim, "", "");
    }

    public static String collectionToCommaDelimitedString(Collection<?> coll) {
        return collectionToDelimitedString(coll, ",");
    }

    public static String collectionToDelimitedString(Collection<?> coll, String delim, String prefix, String suffix) {
        if (CollectionUtils.isEmpty(coll)) {
            return "";
        }

        int totalLength = coll.size() * (prefix.length() + suffix.length()) + (coll.size() - 1) * delim.length();
        for (Object element : coll) {
            totalLength += String.valueOf(element).length();
        }

        StringBuilder sb = new StringBuilder(totalLength);
        Iterator<?> it = coll.iterator();
        while (it.hasNext()) {
            sb.append(prefix).append(it.next()).append(suffix);
            if (it.hasNext()) {
                sb.append(delim);
            }
        }
        return sb.toString();
    }

    public static String arrayToDelimitedString(Object[] arr, String delim) {
        if (ObjectUtils.isEmpty(arr)) {
            return "";
        }
        if (arr.length == 1) {
            return ObjectUtils.nullSafeToString(arr[0]);
        }

        StringJoiner sj = new StringJoiner(delim);
        for (Object elem : arr) {
            sj.add(String.valueOf(elem));
        }
        return sj.toString();
    }

    public static String arrayToCommaDelimitedString(Object[] arr) {
        return arrayToDelimitedString(arr, ",");
    }

    public static String applyRelativePath(String path, String relativePath) {
        int separatorIndex = path.lastIndexOf(FOLDER_SEPARATOR);
        if (separatorIndex != -1) {
            String newPath = path.substring(0, separatorIndex);
            if (!relativePath.startsWith(FOLDER_SEPARATOR)) {
                newPath += FOLDER_SEPARATOR;
            }
            return newPath + relativePath;
        } else {
            return relativePath;
        }
    }


    /**
     * Get the item after one char delim if the delim is found (else null).
     * This operation is a simplified and optimized
     * version of {@link String#split(String, int)}.
     */
    public static String substringAfter(String value, char delim) {
        int pos = value.indexOf(delim);
        if (pos >= 0) {
            return value.substring(pos + 1);
        }
        return null;
    }

    /**
     * Get the item before one char delim if the delim is found (else null).
     * This operation is a simplified and optimized
     * version of {@link String#split(String, int)}.
     */
    public static String substringBefore(String value, char delim) {
        int pos = value.indexOf(delim);
        if (pos >= 0) {
            return value.substring(0, pos);
        }
        return null;
    }

    /**
     * Checks if two strings have the same suffix of specified length
     *
     * @param s   string
     * @param p   string
     * @param len length of the common suffix
     * @return true if both s and p are not null and both have the same suffix. Otherwise - false
     */
    public static boolean commonSuffixOfLength(String s, String p, int len) {
        return s != null && p != null && len >= 0 && s.regionMatches(s.length() - len, p, p.length() - len, len);
    }

    /**
     * Converts the specified byte value into a 2-digit hexadecimal integer.
     */
    public static String byteToHexStringPadded(int value) {
        return BYTE2HEX_PAD[value & 0xff];
    }

    /**
     * Converts the specified byte value into a 2-digit hexadecimal integer and appends it to the specified buffer.
     */
    public static <T extends Appendable> T byteToHexStringPadded(T buf, int value) {
        try {
            buf.append(byteToHexStringPadded(value));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buf;
    }

    /**
     * Converts the specified byte array into a hexadecimal value.
     */
    public static String toHexStringPadded(byte[] src) {
        return toHexStringPadded(src, 0, src.length);
    }

    /**
     * Converts the specified byte array into a hexadecimal value.
     */
    public static String toHexStringPadded(byte[] src, int offset, int length) {
        return toHexStringPadded(new StringBuilder(length << 1), src, offset, length).toString();
    }

    /**
     * Converts the specified byte array into a hexadecimal value and appends it to the specified buffer.
     */
    public static <T extends Appendable> T toHexStringPadded(T dst, byte[] src) {
        return toHexStringPadded(dst, src, 0, src.length);
    }

    /**
     * Converts the specified byte array into a hexadecimal value and appends it to the specified buffer.
     */
    public static <T extends Appendable> T toHexStringPadded(T dst, byte[] src, int offset, int length) {
        final int end = offset + length;
        for (int i = offset; i < end; i++) {
            byteToHexStringPadded(dst, src[i]);
        }
        return dst;
    }

    /**
     * Converts the specified byte value into a hexadecimal integer.
     */
    public static String byteToHexString(int value) {
        return BYTE2HEX_NOPAD[value & 0xff];
    }

    /**
     * Converts the specified byte value into a hexadecimal integer and appends it to the specified buffer.
     */
    public static <T extends Appendable> T byteToHexString(T buf, int value) {
        try {
            buf.append(byteToHexString(value));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buf;
    }

    /**
     * Converts the specified byte array into a hexadecimal value.
     */
    public static String toHexString(byte[] src) {
        return toHexString(src, 0, src.length);
    }

    /**
     * Converts the specified byte array into a hexadecimal value.
     */
    public static String toHexString(byte[] src, int offset, int length) {
        return toHexString(new StringBuilder(length << 1), src, offset, length).toString();
    }

    /**
     * Converts the specified byte array into a hexadecimal value and appends it to the specified buffer.
     */
    public static <T extends Appendable> T toHexString(T dst, byte[] src) {
        return toHexString(dst, src, 0, src.length);
    }

    /**
     * Converts the specified byte array into a hexadecimal value and appends it to the specified buffer.
     */
    public static <T extends Appendable> T toHexString(T dst, byte[] src, int offset, int length) {
        assert length >= 0;
        if (length == 0) {
            return dst;
        }

        final int end = offset + length;
        final int endMinusOne = end - 1;
        int i;

        // Skip preceding zeroes.
        for (i = offset; i < endMinusOne; i++) {
            if (src[i] != 0) {
                break;
            }
        }

        byteToHexString(dst, src[i++]);
        int remaining = end - i;
        toHexStringPadded(dst, src, i, remaining);

        return dst;
    }

    /**
     * Helper to decode half of a hexadecimal number from a string.
     *
     * @param c The ASCII character of the hexadecimal number to decode.
     *          Must be in the range {@code [0-9a-fA-F]}.
     * @return The hexadecimal value represented in the ASCII character
     * given, or {@code -1} if the character is invalid.
     */
    public static int decodeHexNibble(final char c) {
        // Character.digit() is not used here, as it addresses a larger
        // set of characters (both ASCII and full-width latin letters).
        return HEX2B[c];
    }

    /**
     * Helper to decode half of a hexadecimal number from a string.
     *
     * @param b The ASCII character of the hexadecimal number to decode.
     *          Must be in the range {@code [0-9a-fA-F]}.
     * @return The hexadecimal value represented in the ASCII character
     * given, or {@code -1} if the character is invalid.
     */
    public static int decodeHexNibble(final byte b) {
        // Character.digit() is not used here, as it addresses a larger
        // set of characters (both ASCII and full-width latin letters).
        return HEX2B[b];
    }

    /**
     * Decode a 2-digit hex byte from within a string.
     */
    public static byte decodeHexByte(CharSequence s, int pos) {
        int hi = decodeHexNibble(s.charAt(pos));
        int lo = decodeHexNibble(s.charAt(pos + 1));
        if (hi == -1 || lo == -1) {
            throw new IllegalArgumentException(String.format(
                    "invalid hex byte '%s' at index %d of '%s'", s.subSequence(pos, pos + 2), pos, s));
        }
        return (byte) ((hi << 4) + lo);
    }

    /**
     * Decodes part of a string with <a href="https://en.wikipedia.org/wiki/Hex_dump">hex dump</a>
     *
     * @param hexDump   a {@link CharSequence} which contains the hex dump
     * @param fromIndex start of hex dump in {@code hexDump}
     * @param length    hex string length
     */
    public static byte[] decodeHexDump(CharSequence hexDump, int fromIndex, int length) {
        if (length < 0 || (length & 1) != 0) {
            throw new IllegalArgumentException("length: " + length);
        }
        if (length == 0) {
            return EMPTY_BYTES;
        }
        byte[] bytes = new byte[length >>> 1];
        for (int i = 0; i < length; i += 2) {
            bytes[i >>> 1] = decodeHexByte(hexDump, fromIndex + i);
        }
        return bytes;
    }

    /**
     * Decodes a <a href="https://en.wikipedia.org/wiki/Hex_dump">hex dump</a>
     */
    public static byte[] decodeHexDump(CharSequence hexDump) {
        return decodeHexDump(hexDump, 0, hexDump.length());
    }

    /**
     * The shortcut to {@link #simpleClassName(Class) simpleClassName(o.getClass())}.
     */
    public static String simpleClassName(Object o) {
        if (o == null) {
            return "null_object";
        } else {
            return simpleClassName(o.getClass());
        }
    }

    /**
     * Generates a simplified name from a {@link Class}.  Similar to {@link Class#getSimpleName()}, but it works fine
     * with anonymous classes.
     */
    public static String simpleClassName(Class<?> clazz) {
        String className = Assert.checkNotNull(clazz, "clazz").getName();
        final int lastDotIdx = className.lastIndexOf(PACKAGE_SEPARATOR_CHAR);
        if (lastDotIdx > -1) {
            return className.substring(lastDotIdx + 1);
        }
        return className;
    }

    /**
     * Escapes the specified value, if necessary according to
     * <a href="https://tools.ietf.org/html/rfc4180#section-2">RFC-4180</a>.
     *
     * @param value The value which will be escaped according to
     *              <a href="https://tools.ietf.org/html/rfc4180#section-2">RFC-4180</a>
     * @return {@link CharSequence} the escaped value if necessary, or the value unchanged
     */
    public static CharSequence escapeCsv(CharSequence value) {
        return escapeCsv(value, false);
    }

    /**
     * Escapes the specified value, if necessary according to
     * <a href="https://tools.ietf.org/html/rfc4180#section-2">RFC-4180</a>.
     *
     * @param value          The value which will be escaped according to
     *                       <a href="https://tools.ietf.org/html/rfc4180#section-2">RFC-4180</a>
     * @param trimWhiteSpace The value will first be trimmed of its optional white-space characters,
     *                       according to <a href="https://tools.ietf.org/html/rfc7230#section-7">RFC-7230</a>
     * @return {@link CharSequence} the escaped value if necessary, or the value unchanged
     */
    public static CharSequence escapeCsv(CharSequence value, boolean trimWhiteSpace) {
        int length = Assert.checkNotNull(value, "value").length();
        int start;
        int last;
        if (trimWhiteSpace) {
            start = indexOfFirstNonOwsChar(value, length);
            last = indexOfLastNonOwsChar(value, start, length);
        } else {
            start = 0;
            last = length - 1;
        }
        if (start > last) {
            return EMPTY_STRING;
        }

        int firstUnescapedSpecial = -1;
        boolean quoted = false;
        if (isDoubleQuote(value.charAt(start))) {
            quoted = isDoubleQuote(value.charAt(last)) && last > start;
            if (quoted) {
                start++;
                last--;
            } else {
                firstUnescapedSpecial = start;
            }
        }

        if (firstUnescapedSpecial < 0) {
            if (quoted) {
                for (int i = start; i <= last; i++) {
                    if (isDoubleQuote(value.charAt(i))) {
                        if (i == last || !isDoubleQuote(value.charAt(i + 1))) {
                            firstUnescapedSpecial = i;
                            break;
                        }
                        i++;
                    }
                }
            } else {
                for (int i = start; i <= last; i++) {
                    char c = value.charAt(i);
                    if (c == LINE_FEED || c == CARRIAGE_RETURN || c == COMMA) {
                        firstUnescapedSpecial = i;
                        break;
                    }
                    if (isDoubleQuote(c)) {
                        if (i == last || !isDoubleQuote(value.charAt(i + 1))) {
                            firstUnescapedSpecial = i;
                            break;
                        }
                        i++;
                    }
                }
            }

            if (firstUnescapedSpecial < 0) {
                // Special characters is not found or all of them already escaped.
                // In the most cases returns a same string. New string will be instantiated (via StringBuilder)
                // only if it really needed. It's important to prevent GC extra load.
                return quoted ? value.subSequence(start - 1, last + 2) : value.subSequence(start, last + 1);
            }
        }

        StringBuilder result = new StringBuilder(last - start + 1 + CSV_NUMBER_ESCAPE_CHARACTERS);
        result.append(DOUBLE_QUOTE).append(value, start, firstUnescapedSpecial);
        for (int i = firstUnescapedSpecial; i <= last; i++) {
            char c = value.charAt(i);
            if (isDoubleQuote(c)) {
                result.append(DOUBLE_QUOTE);
                if (i < last && isDoubleQuote(value.charAt(i + 1))) {
                    i++;
                }
            }
            result.append(c);
        }
        return result.append(DOUBLE_QUOTE);
    }

    /**
     * Unescapes the specified escaped CSV field, if necessary according to
     * <a href="https://tools.ietf.org/html/rfc4180#section-2">RFC-4180</a>.
     *
     * @param value The escaped CSV field which will be unescaped according to
     *              <a href="https://tools.ietf.org/html/rfc4180#section-2">RFC-4180</a>
     * @return {@link CharSequence} the unescaped value if necessary, or the value unchanged
     */
    public static CharSequence unescapeCsv(CharSequence value) {
        int length = Assert.checkNotNull(value, "value").length();
        if (length == 0) {
            return value;
        }
        int last = length - 1;
        boolean quoted = isDoubleQuote(value.charAt(0)) && isDoubleQuote(value.charAt(last)) && length != 1;
        if (!quoted) {
            validateCsvFormat(value);
            return value;
        }
        StringBuilder unescaped = new StringBuilder();
        for (int i = 1; i < last; i++) {
            char current = value.charAt(i);
            if (current == DOUBLE_QUOTE) {
                if (isDoubleQuote(value.charAt(i + 1)) && (i + 1) != last) {
                    // Followed by a double-quote but not the last character
                    // Just skip the next double-quote
                    i++;
                } else {
                    // Not followed by a double-quote or the following double-quote is the last character
                    throw newInvalidEscapedCsvFieldException(value, i);
                }
            }
            unescaped.append(current);
        }
        return unescaped.toString();
    }

    /**
     * Unescapes the specified escaped CSV fields according to
     * <a href="https://tools.ietf.org/html/rfc4180#section-2">RFC-4180</a>.
     *
     * @param value A string with multiple CSV escaped fields which will be unescaped according to
     *              <a href="https://tools.ietf.org/html/rfc4180#section-2">RFC-4180</a>
     * @return {@link List} the list of unescaped fields
     */
    public static List<CharSequence> unescapeCsvFields(CharSequence value) {
        List<CharSequence> unescaped = new ArrayList<CharSequence>(2);
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        int last = value.length() - 1;
        for (int i = 0; i <= last; i++) {
            char c = value.charAt(i);
            if (quoted) {
                switch (c) {
                    case DOUBLE_QUOTE:
                        if (i == last) {
                            // Add the last field and return
                            unescaped.add(current.toString());
                            return unescaped;
                        }
                        char next = value.charAt(++i);
                        if (next == DOUBLE_QUOTE) {
                            // 2 double-quotes should be unescaped to one
                            current.append(DOUBLE_QUOTE);
                            break;
                        }
                        if (next == COMMA) {
                            // This is the end of a field. Let's start to parse the next field.
                            quoted = false;
                            unescaped.add(current.toString());
                            current.setLength(0);
                            break;
                        }
                        // double-quote followed by other character is invalid
                        throw newInvalidEscapedCsvFieldException(value, i - 1);
                    default:
                        current.append(c);
                }
            } else {
                switch (c) {
                    case COMMA:
                        // Start to parse the next field
                        unescaped.add(current.toString());
                        current.setLength(0);
                        break;
                    case DOUBLE_QUOTE:
                        if (current.length() == 0) {
                            quoted = true;
                            break;
                        }
                        // double-quote appears without being enclosed with double-quotes
                        // fall through
                    case LINE_FEED:
                        // fall through
                    case CARRIAGE_RETURN:
                        // special characters appears without being enclosed with double-quotes
                        throw newInvalidEscapedCsvFieldException(value, i);
                    default:
                        current.append(c);
                }
            }
        }
        if (quoted) {
            throw newInvalidEscapedCsvFieldException(value, last);
        }
        unescaped.add(current.toString());
        return unescaped;
    }

    /**
     * Validate if {@code value} is a valid csv field without double-quotes.
     *
     * @throws IllegalArgumentException if {@code value} needs to be encoded with double-quotes.
     */
    private static void validateCsvFormat(CharSequence value) {
        int length = value.length();
        for (int i = 0; i < length; i++) {
            switch (value.charAt(i)) {
                case DOUBLE_QUOTE:
                case LINE_FEED:
                case CARRIAGE_RETURN:
                case COMMA:
                    // If value contains any special character, it should be enclosed with double-quotes
                    throw newInvalidEscapedCsvFieldException(value, i);
                default:
            }
        }
    }

    private static IllegalArgumentException newInvalidEscapedCsvFieldException(CharSequence value, int index) {
        return new IllegalArgumentException("invalid escaped CSV field: " + value + " index: " + index);
    }

    /**
     * Get the length of a string, {@code null} input is considered {@code 0} length.
     */
    public static int length(String s) {
        return s == null ? 0 : s.length();
    }

    /**
     * Determine if a string is {@code null} or {@link String#isEmpty()} returns {@code true}.
     */
    public static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    /**
     * Find the index of the first non-white space character in {@code s} starting at {@code offset}.
     *
     * @param seq    The string to search.
     * @param offset The offset to start searching at.
     * @return the index of the first non-white space character or &lt;{@code -1} if none was found.
     */
    public static int indexOfNonWhiteSpace(CharSequence seq, int offset) {
        for (; offset < seq.length(); ++offset) {
            if (!Character.isWhitespace(seq.charAt(offset))) {
                return offset;
            }
        }
        return -1;
    }

    /**
     * Find the index of the first white space character in {@code s} starting at {@code offset}.
     *
     * @param seq    The string to search.
     * @param offset The offset to start searching at.
     * @return the index of the first white space character or &lt;{@code -1} if none was found.
     */
    public static int indexOfWhiteSpace(CharSequence seq, int offset) {
        for (; offset < seq.length(); ++offset) {
            if (Character.isWhitespace(seq.charAt(offset))) {
                return offset;
            }
        }
        return -1;
    }

    /**
     * Determine if {@code c} lies within the range of values defined for
     * <a href="https://unicode.org/glossary/#surrogate_code_point">Surrogate Code Point</a>.
     *
     * @param c the character to check.
     * @return {@code true} if {@code c} lies within the range of values defined for
     * <a href="https://unicode.org/glossary/#surrogate_code_point">Surrogate Code Point</a>. {@code false} otherwise.
     */
    public static boolean isSurrogate(char c) {
        return c >= '\uD800' && c <= '\uDFFF';
    }

    private static boolean isDoubleQuote(char c) {
        return c == DOUBLE_QUOTE;
    }

    /**
     * Determine if the string {@code s} ends with the char {@code c}.
     *
     * @param s the string to test
     * @param c the tested char
     * @return true if {@code s} ends with the char {@code c}
     */
    public static boolean endsWith(CharSequence s, char c) {
        int len = s.length();
        return len > 0 && s.charAt(len - 1) == c;
    }

    /**
     * Trim optional white-space characters from the specified value,
     * according to <a href="https://tools.ietf.org/html/rfc7230#section-7">RFC-7230</a>.
     *
     * @param value the value to trim
     * @return {@link CharSequence} the trimmed value if necessary, or the value unchanged
     */
    public static CharSequence trimOws(CharSequence value) {
        final int length = value.length();
        if (length == 0) {
            return value;
        }
        int start = indexOfFirstNonOwsChar(value, length);
        int end = indexOfLastNonOwsChar(value, start, length);
        return start == 0 && end == length - 1 ? value : value.subSequence(start, end + 1);
    }

    /**
     * Returns a char sequence that contains all {@code elements} joined by a given separator.
     *
     * @param separator for each element
     * @param elements  to join together
     * @return a char sequence joined by a given separator.
     */
    public static CharSequence join(CharSequence separator, Iterable<? extends CharSequence> elements) {
        Assert.checkNotNull(separator, "separator");
        Assert.checkNotNull(elements, "elements");

        Iterator<? extends CharSequence> iterator = elements.iterator();
        if (!iterator.hasNext()) {
            return EMPTY_STRING;
        }

        CharSequence firstElement = iterator.next();
        if (!iterator.hasNext()) {
            return firstElement;
        }

        StringBuilder builder = new StringBuilder(firstElement);
        do {
            builder.append(separator).append(iterator.next());
        } while (iterator.hasNext());

        return builder;
    }

    /**
     * @return {@code length} if no OWS is found.
     */
    private static int indexOfFirstNonOwsChar(CharSequence value, int length) {
        int i = 0;
        while (i < length && isOws(value.charAt(i))) {
            i++;
        }
        return i;
    }

    /**
     * @return {@code start} if no OWS is found.
     */
    private static int indexOfLastNonOwsChar(CharSequence value, int start, int length) {
        int i = length - 1;
        while (i > start && isOws(value.charAt(i))) {
            i--;
        }
        return i;
    }

    private static boolean isOws(char c) {
        return c == SPACE || c == TAB;
    }


    public static boolean equals(String str1, String str2) {
        boolean ret = false;

        if (str1 == null) {
            ret = str2 == null;
        } else if (str2 == null) {
            ret = false;
        } else {
            ret = str1.equals(str2);
        }

        return ret;
    }

    public static boolean equalsIgnoreCase(String str1, String str2) {
        boolean ret = false;

        if (str1 == null) {
            ret = str2 == null;
        } else if (str2 == null) {
            ret = false;
        } else {
            ret = str1.equalsIgnoreCase(str2);
        }

        return ret;
    }

    public static boolean equals(Collection<String> set1, Collection<String> set2) {
        boolean ret = false;

        if (set1 == null) {
            ret = set2 == null;
        } else if (set2 == null) {
            ret = false;
        } else if (set1.size() == set2.size()) {
            ret = set1.containsAll(set2);
        }

        return ret;
    }

    public static boolean equalsIgnoreCase(Collection<String> set1, Collection<String> set2) {
        boolean ret = false;

        if (set1 == null) {
            ret = set2 == null;
        } else if (set2 == null) {
            ret = false;
        } else if (set1.size() == set2.size()) {
            int numFound = 0;

            for (String str1 : set1) {
                boolean str1Found = false;

                for (String str2 : set2) {
                    if (equalsIgnoreCase(str1, str2)) {
                        str1Found = true;

                        break;
                    }
                }

                if (str1Found) {
                    numFound++;
                } else {
                    break;
                }
            }

            ret = numFound == set1.size();
        }

        return ret;
    }

    public static boolean matches(String pattern, String str) {
        boolean ret = false;

        if (pattern == null || str == null || pattern.isEmpty() || str.isEmpty()) {
            ret = true;
        } else {
            ret = str.matches(pattern);
        }

        return ret;
    }

    public static boolean contains(String str, String strToFind) {
        return str != null && strToFind != null && str.contains(strToFind);
    }

    public static boolean containsIgnoreCase(String str, String strToFind) {
        return str != null && strToFind != null && str.toLowerCase().contains(strToFind.toLowerCase());
    }

    public static boolean contains(String[] strArr, String str) {
        boolean ret = false;

        if (strArr != null && strArr.length > 0 && str != null) {
            for (String s : strArr) {
                ret = equals(s, str);

                if (ret) {
                    break;
                }
            }
        }

        return ret;
    }

    public static boolean containsIgnoreCase(String[] strArr, String str) {
        boolean ret = false;

        if (strArr != null && strArr.length > 0 && str != null) {
            for (String s : strArr) {
                ret = equalsIgnoreCase(s, str);

                if (ret) {
                    break;
                }
            }
        }

        return ret;
    }

    public static String toString(Iterable<String> iterable) {
        String ret = "";

        if (iterable != null) {
            int count = 0;
            for (String str : iterable) {
                if (count == 0)
                    ret = str;
                else
                    ret += (", " + str);
                count++;
            }
        }

        return ret;
    }

    public static String toString(String[] arr) {
        String ret = "";

        if (arr != null && arr.length > 0) {
            ret = arr[0];
            for (int i = 1; i < arr.length; i++) {
                ret += (", " + arr[i]);
            }
        }

        return ret;
    }

    public static String toString(List<String> arr) {
        String ret = "";

        if (arr != null && !arr.isEmpty()) {
            ret = arr.get(0);
            for (int i = 1; i < arr.size(); i++) {
                ret += (", " + arr.get(i));
            }
        }

        return ret;
    }

    /**
     * 지정한 문자열을 축약한다.
     *
     * @param str 축약할 문자열
     * @param len 문자열 길이
     * @return 축약한 문자열
     */
    public static String abbreviateString(String str, int len) {
        if (str == null) {
            return "null";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        if (str.length() > len - 2) {
            sb.append(str.substring(0, len - 5)).append("...");
        } else {
            sb.append(str);
        }
        sb.append("\"");

        return sb.toString();
    }
}
