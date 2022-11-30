package com.gargle.common.utils.http;

import com.alibaba.fastjson.JSONObject;
import com.gargle.common.exception.GargleException;
import com.gargle.common.utils.http.base.Response;
import com.gargle.common.utils.string.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.*;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.Locale;
import java.util.Map;

/**
 * ClassName:HTTPUtil
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/11/30 10:28
 */
public final class HTTPUtil {

    private static final Logger logger = LoggerFactory.getLogger(HTTPUtil.class);

    public Response<String> send(
            String url,
            String method,
            String contentType,
            Integer readTimeout,
            Integer connectTimeout,
            Map<String, Object> urlParams,
            Map<String, Object> bodyParams,
            Map<String, String> headerParams) {
        logger.info("请求开始, url: {}, urlParam: {}, bodyParam:{}, headerParam:{}",
                url,
                JSONObject.toJSONString(urlParams),
                JSONObject.toJSONString(bodyParams),
                JSONObject.toJSONString(headerParams));
        if (StringUtil.isBlank(url)) {
            logger.error("url 为空.");
            return Response.error("url 为空.");
        }
        try {
            url = buildUrlParam(url, urlParams);
        } catch (Exception e) {
            logger.error("构建url参数异常url: {}, urlParam: {}", url, JSONObject.toJSONString(urlParams), e);
            return Response.error("构建url参数异常," + e.getMessage());
        }

        String bodyParam;

        try {
            bodyParam = buildParam(bodyParams);
        } catch (Exception e) {
            logger.error("构建body参数异常url: {}, bodyParam: {}", url, JSONObject.toJSONString(bodyParams), e);
            return Response.error("构建body参数异常, " + e.getMessage());
        }

        PrintWriter out = null;
        BufferedReader in = null;
        StringBuilder result = new StringBuilder();
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            HttpURLConnection conn = (HttpURLConnection) realUrl.openConnection();
            // trust-https
            boolean useHttps = url.startsWith("https");
            if (useHttps) {
                HttpsURLConnection https = (HttpsURLConnection) conn;
                trustAllHosts(https);
            }

            // 设置请求方式
            conn.setRequestMethod(method);
            if (readTimeout != null) {
                if (readTimeout <= 0) {
                    throw new GargleException("读取超时时间配置异常, 小于等于 0, readTimeout: " + readTimeout);
                }
                conn.setReadTimeout(readTimeout);
            } else {
                conn.setReadTimeout(60_000);
            }

            if (connectTimeout != null) {
                if (connectTimeout <= 0) {
                    throw new GargleException("连接超时时间配置异常, 小于等于 0, connectTimeout: " + connectTimeout);
                }
                conn.setConnectTimeout(connectTimeout);
            } else {
                conn.setConnectTimeout(60_000);
            }


            conn.setUseCaches(false);
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.addRequestProperty("Content-type", contentType);
            conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 设置外部配置请求属性.
            if (headerParams != null && headerParams.size() > 0) {
                for (Map.Entry<String, String> entry : headerParams.entrySet()) {
                    conn.addRequestProperty(entry.getKey(), entry.getValue());
                }
            }


            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            if ("post".equals(method.trim().toLowerCase(Locale.ROOT))) {
                out = new PrintWriter(new OutputStreamWriter(conn.getOutputStream(), StandardCharsets.UTF_8));
                // 发送请求参数
                if (StringUtil.isNotBlank(bodyParam)) {
                    out.print(bodyParam);
                }
                // flush输出流的缓冲
                out.flush();
                // 定义BufferedReader输入流来读取URL的响应
                in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                String line;
                while ((line = in.readLine()) != null) {
                    result.append(line);
                }
            } else {
                //连接服务器
                conn.connect();
                // 取得输入流，并使用Reader读取
                in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                String line;
                while ((line = in.readLine()) != null) {
                    result.append(line);
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("发送请求出现异常", e);
            return Response.error("404");
        } catch (ConnectException e) {
            logger.error("发送请求出现异常", e);
            if (e.getMessage().contains("Connection refused")) {
                return Response.error("Connection refused," + e.getMessage());
            } else {
                return Response.error("ResponseEnum.HTTP_ERROR" + e.getMessage());
            }
        } catch (SocketTimeoutException e) {
            logger.error("发送请求出现异常", e);
            if (e.getMessage().contains("Read timed out")) {
                return Response.error("Read timed out," + e.getMessage());
            }

            if (e.getMessage().contains("connect timed out")) {
                return Response.error("connect timed out, " + e.getMessage());
            }

            return Response.error("socket超时.");
        } catch (Exception e) {
            logger.error("发送请求出现异常", e);
            return Response.error("http未知异常," + e.getMessage());
        }
        //使用finally块来关闭输出流、输入流
        finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                logger.error("连接关闭异常.", ex);
            }

        }
        return Response.success(result.toString());
    }

    private static String buildUrlParam(String url, Map<String, Object> params) {
        String s = buildParam(params);
        return url + "?" + s;
    }

    private static String buildParam(Map<String, Object> params) {
        if (params == null || params.size() == 0) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        int i = 1;
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            stringBuilder.append(entry.getKey()).append("=").append(entry.getValue());
            if (i >= params.size()) {
                continue;
            }

            stringBuilder.append("&");
        }


        return stringBuilder.toString();
    }

    private void trustAllHosts(HttpsURLConnection connection) {
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            SSLSocketFactory newFactory = sc.getSocketFactory();

            connection.setSSLSocketFactory(newFactory);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        connection.setHostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        });
    }

    private static final TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[]{};
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }
    }};
}
