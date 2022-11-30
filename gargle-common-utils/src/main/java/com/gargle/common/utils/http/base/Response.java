package com.gargle.common.utils.http.base;

import com.gargle.common.utils.string.StringUtil;

import java.io.Serializable;

/**
 * ClassName:Response
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/11/30 10:30
 */
public class Response<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    private int code;
    private String message;
    private boolean success;
    /**
     * 三方返回数据
     */
    private T data;

    public Response() {
    }

    public Response(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public static <T> Response<T> success() {
        Response<T> response = new Response<T>();
        response.setSuccess(true);
        response.setMessage("成功");
        response.setCode(200);
        return response;
    }


    public static <T> Response<T> success(T data) {
        Response<T> response = new Response<T>();
        response.setSuccess(true);
        response.setMessage("成功");
        response.setCode(200);
        if (data != null) {
            response.setData(data);
        }
        return response;
    }

    public static <T> Response<T> error() {
        return error("执行失败");
    }

    public static <T> Response<T> error(String message) {
        Response<T> response = new Response<T>();
        response.setSuccess(false);
        response.setCode(500);
        if (StringUtil.isNotBlank(message) ) {
            response.setMessage(message);
        } else {
            response.setMessage("执行失败!");
        }
        return response;
    }


    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
