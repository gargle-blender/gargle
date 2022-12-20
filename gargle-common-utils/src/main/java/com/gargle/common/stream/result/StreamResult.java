package com.gargle.common.stream.result;

import com.alibaba.fastjson.JSONObject;
import com.gargle.common.enumeration.stream.StreamCodeEnum;
import com.gargle.common.serializable.Serializable;
import lombok.Data;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * ClassName:StreamResult
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/12/05 14:42
 */
@Data
public class StreamResult implements Serializable {
    private boolean success = true;

    private String generationTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

    /**
     * 响应信息
     */
    private String message = StreamCodeEnum.CODE_SUCCESS.getMsg();

    /**
     * 响应编码
     */
    private Integer code = StreamCodeEnum.CODE_SUCCESS.getCode();

    /**
     * 异常链路信息
     */
    private List<Exception> exceptions = new CopyOnWriteArrayList<>();
    private List<String> errorMessages = new CopyOnWriteArrayList<>();

    /**
     * 进件原始信息
     */
    private Object recode;

    /**
     * 执行链路头节点名称
     */
    private String firstNodeName;

    public static StreamResult success() {
        return new StreamResult();
    }

    public static StreamResult fail(Exception e, String recode, String errorMessage) {
        StreamResult handleResult = new StreamResult();
        handleResult.setSuccess(false);
        handleResult.setCode(StreamCodeEnum.CODE_FAIL.getCode());
        handleResult.setMessage(StreamCodeEnum.CODE_FAIL.getMsg());
        handleResult.setRecode(recode);
        if (e != null) {
            handleResult.getExceptions().add(e);
        }
        handleResult.getErrorMessages().add(errorMessage);
        return handleResult;
    }

    public static StreamResult fail(Exception e, String errorMessage) {
        StreamResult handleResult = new StreamResult();
        handleResult.setSuccess(false);
        handleResult.setCode(StreamCodeEnum.CODE_FAIL.getCode());
        handleResult.setMessage(StreamCodeEnum.CODE_FAIL.getMsg());
        if (e != null) {
            handleResult.getExceptions().add(e);
        }
        handleResult.getErrorMessages().add(errorMessage);
        return handleResult;
    }

    public static StreamResult fail(Exception e, String errorMessage, StreamCodeEnum codeEnum) {
        StreamResult handleResult = new StreamResult();
        handleResult.setSuccess(false);
        handleResult.setCode(codeEnum.getCode());
        handleResult.setMessage(codeEnum.getMsg());
        if (e != null) {
            handleResult.getExceptions().add(e);
        }
        handleResult.getErrorMessages().add(errorMessage);
        return handleResult;
    }

    public static StreamResult fail(String errorMessage) {
        StreamResult handleResult = new StreamResult();
        handleResult.setSuccess(false);
        handleResult.setCode(StreamCodeEnum.CODE_FAIL.getCode());
        handleResult.setMessage(StreamCodeEnum.CODE_FAIL.getMsg());
        handleResult.getErrorMessages().add(errorMessage);
        return handleResult;
    }


    public static StreamResult fail(String errorMessage, String recode, String producerName) {
        StreamResult handleResult = new StreamResult();
        handleResult.setSuccess(false);
        handleResult.setCode(StreamCodeEnum.CODE_FAIL.getCode());
        handleResult.setMessage(StreamCodeEnum.CODE_FAIL.getMsg());
        handleResult.getErrorMessages().add(errorMessage);
        handleResult.setRecode(recode);
        return handleResult;
    }

    public static StreamResult end() {
        StreamResult handleResult = new StreamResult();
        handleResult.setSuccess(true);
        handleResult.setCode(StreamCodeEnum.CODE_END.getCode());
        handleResult.setMessage(StreamCodeEnum.CODE_END.getMsg());
        return handleResult;
    }

    @Override
    public String toString() {
        return JSONObject.toJSONString(this);
    }
}