package com.gargle.common.utils.string;

import java.util.regex.Pattern;

/**
 * ClassName:StringUtil
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/11/29 14:36
 */
public final class StringUtil {

    private static final int A = 65;
    private static final int Z = 90;
    private static final int a = 97;
    private static final int z = 122;
    private static final int ZERO = 0;
    private static final int TEMP = 32;

    /**
     * 数字
     */
    public static final String NUMBER = "0123456789";
    /**
     * 字母
     */
    public static final String ALPHABET = "abcdefghijklmnopqrstuvwyxz";
    /**
     * 符号
     */
    public static final String SYMBOL = "~!@#$%^&*()_+[]{};,.<>?-=";

    /**
     * 将第一个为小写字母的值改为对应的大写字母.
     */
    public static String toUpperCaseFirstIgnoreOther(String target) {
        if (StringUtil.isBlank(target)) {
            return target;
        }

        char[] chars = target.toCharArray();
        for (int i = ZERO; i < chars.length; i++) {
            if (isLowerCase(chars[i])) {
                chars[i] = (char) (chars[i] - TEMP);
                break;
            }
        }

        return new String(chars);

    }

    /**
     * 首字母大写,首位必须是字母
     *
     * @param target 目标字符串
     * @return 首字母已经是大写字母 或者 target 为空或空字符串 返回 target. 否则将首字母大写返回.
     */
    public static String toUpperCaseFirst(String target) {
        if (isBlank(target)) {
            return target;
        }

        char[] chars = target.toCharArray();
        // 判断是否已经是大写字母
        if (isUpperCase(chars[ZERO])) {
            return target;
        }
        // 判断是否是小写字母
        if (isLowerCase(chars[ZERO])) {
            chars[ZERO] = (char) (chars[ZERO] - TEMP);
            return new String(chars);
        }

        return target;
    }

    /**
     * 将第一个为大写字母的值改为对应的小写字母.
     *
     * @param target 目标字符串
     * @return 若目标字符串为空或者为空字符串, 直接返回target.
     */
    public static String toLowerCaseFirstIgnoreOther(String target) {
        if (isBlank(target)) {
            return target;
        }

        char[] chars = target.toCharArray();
        for (int i = ZERO; i < chars.length; i++) {
            if (isUpperCase(chars[i])) {
                chars[i] = (char) (chars[i] + TEMP);
                break;
            }
        }

        return new String(chars);
    }

    /**
     * 首字母小写,首位必须是字母
     *
     * @param target 目标字符串
     * @return 首字母已经是小写字母 或者 target 为空或空字符串 返回 target. 否则将首字母小写返回.
     */
    public static String toLowerCaseFirst(String target) {
        if (isBlank(target)) {
            return target;
        }

        char[] chars = target.toCharArray();
        // 判断是否已经是小写字母
        if (isLowerCase(chars[ZERO])) {
            return target;
        }
        // 判断是否是大写字母
        if (isUpperCase(chars[ZERO])) {
            chars[ZERO] = (char) (chars[ZERO] - TEMP);
            return new String(chars);
        }
        return target;
    }

    /**
     * 判断某个字符是否是小写字母.
     *
     * @param c 需要判断的字符
     * @return 是: true    否:false
     */
    public static boolean isLowerCase(char c) {
        return c >= a && c <= z;
    }

    /**
     * 判断某个字符串首字母是否是小写字母.
     *
     * @param target 需要判断的字符串
     * @return 是: true    否:false
     */
    public static boolean isLowerCaseFirst(String target) {
        char firstChar = getFirstChar(target);
        return firstChar >= a && firstChar <= z;
    }

    /**
     * 判断某个字符是否是大写字母.
     *
     * @param c 需要判断的字符
     * @return 是: true    否:false
     */
    public static boolean isUpperCase(char c) {
        return c >= A && c <= Z;
    }

    /**
     * 判断某个字符串首字母是否是大写字母.
     *
     * @param target 需要判断的字符串
     * @return 是: true    否:false
     */
    public static boolean isUpperCaseFirst(String target) {
        char firstChar = getFirstChar(target);
        return firstChar >= A && firstChar <= Z;
    }

    /**
     * @param target 目标字符串
     * @return 目标字符串的首字符.
     */
    public static char getFirstChar(String target) {
        if (target == null) {
            throw new NullPointerException("target is null.");
        }

        char[] chars = target.toCharArray();
        if (chars.length == ZERO) {
            throw new NullPointerException("target size is 0.");
        }

        return chars[ZERO];
    }

    /**
     * @param target 目标字符串
     * @return 目标字符串的首字符.
     */
    public static String getFirstCharToString(String target) {
        if (target == null) {
            throw new NullPointerException("target is null.");
        }

        char[] chars = target.toCharArray();
        if (chars.length == ZERO) {
            throw new NullPointerException("target size is 0.");
        }

        return String.valueOf(chars[ZERO]);
    }

    public static boolean isBlank(CharSequence cs) {
        int strLen;
        if (cs != null && (strLen = cs.length()) != ZERO) {
            for (int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(cs.charAt(i))) {
                    return false;
                }
            }
            return true;
        } else {
            return true;
        }
    }

    public static boolean isNotBlank(CharSequence cs) {
        return !isBlank(cs);
    }

    /**
     * 根据所传的正则匹配手机号字符串
     *
     * @throws NullPointerException target 为 null 时抛出此异常
     */
    public static boolean matchMobile(String pattern, String target) {
        return Pattern.matches(pattern, target);
    }

    /**
     * 将Unicode编码转为字符串
     *
     * @param unicode
     * @return
     */
    public static String decodeUnicode(String unicode) {
        if (!unicode.contains("\\u")) {
            return unicode;
        }
        StringBuffer string = new StringBuffer();
        String[] hex = unicode.split("\\\\u");
        for (int i = 1; i < hex.length; i++) {
            int data = Integer.parseInt(hex[i], 16);
            string.append((char) data);
        }
        return string.toString();
    }


}