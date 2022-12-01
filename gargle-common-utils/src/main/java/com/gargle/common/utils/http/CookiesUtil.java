package com.gargle.common.utils.http;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 * ClassName:DateConstant
 * Description:
 *
 * @author qingwen.shang
 */
public final class CookiesUtil {

    /**
     * 根据名字获取cookie
     */
    public static Cookie getCookieByName(HttpServletRequest request, String cookieName) {
        Map<String, Cookie> cookieMap = readCookieMap(request);
        return cookieMap.get(cookieName);
    }

    /**
     * 将cookie封装到Map里面
     */
    private static Map<String, Cookie> readCookieMap(HttpServletRequest request) {
        Map<String, Cookie> cookieMap = new HashMap<>();
        Cookie[] cookies = request.getCookies();
        if (null != cookies) {
            for (Cookie cookie : cookies) {
                cookieMap.put(cookie.getName(), cookie);
            }
        }
        return cookieMap;
    }

    /**
     * 保存Cookies
     *
     * @param response servlet请求
     * @param value    保存值
     * @return HttpServletResponse
     */
    public static HttpServletResponse setCookie(HttpServletResponse response, String name, String value, int time) throws UnsupportedEncodingException {
        // new一个Cookie对象,键值对为参数
        Cookie cookie = new Cookie(name, value);
        // tomcat下多应用共享
        cookie.setPath("/");
        URLEncoder.encode(value, "utf-8");
        cookie.setMaxAge(time);
        // 将Cookie添加到Response中,使之生效
        // addCookie后，如果已经存在相同名字的cookie，则最新的覆盖旧的cookie
        response.addCookie(cookie);
        return response;
    }

    /**
     * 删除cookie
     */
    public void deleteCookieByName(HttpServletRequest request, HttpServletResponse response, String cookieName) {
        Map<String, Cookie> cookieMap = readCookieMap(request);
        for (Map.Entry<String, Cookie> entry : cookieMap.entrySet()) {
            final String key = entry.getKey();
            if (key.equals(cookieName)) {
                Cookie cookie = cookieMap.get(key);
                //设置cookie有效时间为0
                cookie.setMaxAge(0);
                //不设置存储路径
                cookie.setPath("/");
                response.addCookie(cookie);
            }
        }
    }
}
