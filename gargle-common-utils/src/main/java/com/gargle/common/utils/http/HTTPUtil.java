package com.gargle.common.utils.http;

import com.alibaba.fastjson.JSONObject;
import com.gargle.common.enumeration.http.ContentTypeEnum;
import com.gargle.common.enumeration.http.MethodTypeEnum;
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
 */
public final class HTTPUtil {

    private static final Logger logger = LoggerFactory.getLogger(HTTPUtil.class);

    public Response<String> sendGet(
            String url,
            Map<String, Object> urlParams) {
        return send(url, MethodTypeEnum.GET, ContentTypeEnum.APPLICATION_JSON, 60_000, 60_000,
                urlParams, null, null);
    }

    public Response<String> sendGet(
            String url,
            Map<String, Object> urlParams,
            Map<String, String> headerParams) {
        return send(url, MethodTypeEnum.GET, ContentTypeEnum.APPLICATION_JSON, 60_000, 60_000,
                urlParams, null, headerParams);
    }

    public Response<String> sendGet(
            String url,
            ContentTypeEnum contentType,
            Integer readTimeout,
            Integer connectTimeout,
            Map<String, Object> urlParams,
            Map<String, Object> bodyParams,
            Map<String, String> headerParams) {
        return send(url, MethodTypeEnum.GET, contentType, readTimeout, connectTimeout,
                urlParams, bodyParams, headerParams);
    }

    public Response<String> sendPost(
            String url,
            Map<String, Object> bodyParams) {
        return send(url, MethodTypeEnum.POST, ContentTypeEnum.APPLICATION_X_WWW_FORM_URLENCODED,
                60_000, 60_000,
                null, bodyParams, null);
    }

    public Response<String> sendPost(
            String url,
            Map<String, Object> bodyParams,
            Map<String, String> headerParams) {
        return send(url, MethodTypeEnum.POST, ContentTypeEnum.APPLICATION_X_WWW_FORM_URLENCODED,
                60_000, 60_000,
                null, bodyParams, headerParams);
    }

    public Response<String> sendPost(
            String url,
            ContentTypeEnum contentType,
            Integer readTimeout,
            Integer connectTimeout,
            Map<String, Object> urlParams,
            Map<String, Object> bodyParams,
            Map<String, String> headerParams) {
        return send(url, MethodTypeEnum.POST, contentType, readTimeout, connectTimeout,
                urlParams, bodyParams, headerParams);
    }

    public Response<String> send(
            String url,
            MethodTypeEnum methodType,
            ContentTypeEnum contentType,
            Integer readTimeout,
            Integer connectTimeout,
            Map<String, Object> urlParams,
            Map<String, Object> bodyParams,
            Map<String, String> headerParams) {
        return send(url, methodType.name(), contentType.getCode(), readTimeout, connectTimeout,
                urlParams, bodyParams, headerParams);
    }

    public Response<String> send(
            String url,
            String method,
            String contentType,
            Integer readTimeout,
            Integer connectTimeout,
            Map<String, Object> urlParams,
            Map<String, Object> bodyParams,
            Map<String, String> headerParams) {
        logger.info("????????????, url: {}, urlParam: {}, bodyParam:{}, headerParam:{}",
                url,
                JSONObject.toJSONString(urlParams),
                JSONObject.toJSONString(bodyParams),
                JSONObject.toJSONString(headerParams));
        if (StringUtil.isBlank(url)) {
            logger.error("url ??????.");
            return Response.error("url ??????.");
        }
        try {
            url = buildUrlParam(url, urlParams);
        } catch (Exception e) {
            logger.error("??????url????????????url: {}, urlParam: {}", url, JSONObject.toJSONString(urlParams), e);
            return Response.error("??????url????????????," + e.getMessage());
        }

        String bodyParam;

        try {
            bodyParam = buildParam(bodyParams);
        } catch (Exception e) {
            logger.error("??????body????????????url: {}, bodyParam: {}", url, JSONObject.toJSONString(bodyParams), e);
            return Response.error("??????body????????????, " + e.getMessage());
        }

        PrintWriter out = null;
        BufferedReader in = null;
        StringBuilder result = new StringBuilder();
        try {
            URL realUrl = new URL(url);
            // ?????????URL???????????????
            HttpURLConnection conn = (HttpURLConnection) realUrl.openConnection();
            // trust-https
            boolean useHttps = url.startsWith("https");
            if (useHttps) {
                HttpsURLConnection https = (HttpsURLConnection) conn;
                trustAllHosts(https);
            }

            // ??????????????????
            conn.setRequestMethod(method);
            if (readTimeout != null) {
                if (readTimeout <= 0) {
                    throw new GargleException("??????????????????????????????, ???????????? 0, readTimeout: " + readTimeout);
                }
                conn.setReadTimeout(readTimeout);
            } else {
                conn.setReadTimeout(60_000);
            }

            if (connectTimeout != null) {
                if (connectTimeout <= 0) {
                    throw new GargleException("??????????????????????????????, ???????????? 0, connectTimeout: " + connectTimeout);
                }
                conn.setConnectTimeout(connectTimeout);
            } else {
                conn.setConnectTimeout(60_000);
            }


            conn.setUseCaches(false);
            // ???????????????????????????
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.addRequestProperty("Content-type", contentType);
            conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // ??????????????????????????????.
            if (headerParams != null && headerParams.size() > 0) {
                for (Map.Entry<String, String> entry : headerParams.entrySet()) {
                    conn.addRequestProperty(entry.getKey(), entry.getValue());
                }
            }


            // ??????POST??????????????????????????????
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // ??????URLConnection????????????????????????
            if ("post".equals(method.trim().toLowerCase(Locale.ROOT))) {
                out = new PrintWriter(new OutputStreamWriter(conn.getOutputStream(), StandardCharsets.UTF_8));
                // ??????????????????
                if (StringUtil.isNotBlank(bodyParam)) {
                    out.print(bodyParam);
                }
                // flush??????????????????
                out.flush();
                // ??????BufferedReader??????????????????URL?????????
                in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                String line;
                while ((line = in.readLine()) != null) {
                    result.append(line);
                }
            } else {
                //???????????????
                conn.connect();
                // ???????????????????????????Reader??????
                in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                String line;
                while ((line = in.readLine()) != null) {
                    result.append(line);
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("????????????????????????", e);
            return Response.error("404");
        } catch (ConnectException e) {
            logger.error("????????????????????????", e);
            if (e.getMessage().contains("Connection refused")) {
                return Response.error("Connection refused," + e.getMessage());
            } else {
                return Response.error("ResponseEnum.HTTP_ERROR" + e.getMessage());
            }
        } catch (SocketTimeoutException e) {
            logger.error("????????????????????????", e);
            if (e.getMessage().contains("Read timed out")) {
                return Response.error("Read timed out," + e.getMessage());
            }

            if (e.getMessage().contains("connect timed out")) {
                return Response.error("connect timed out, " + e.getMessage());
            }

            return Response.error("socket??????.");
        } catch (Exception e) {
            logger.error("????????????????????????", e);
            return Response.error("http????????????," + e.getMessage());
        }
        //??????finally?????????????????????????????????
        finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                logger.error("??????????????????.", ex);
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
        connection.setHostnameVerifier((hostname, session) -> true);
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
