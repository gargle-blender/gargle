package com.gargle.common.enumeration.stream;

/**
 * ClassName:CodeEnum
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/04/18 09:39
 */
public enum StreamCodeEnum {


    CODE_SUCCESS("success", 200),
    CODE_END("end", 201),
    CODE_FAIL("fail", 500),
    CODE_FAIL_NO_RESEND("fail but cannot resend", 501);

    private String msg;

    private Integer code;

    StreamCodeEnum(String msg, Integer code) {
        this.msg = msg;
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }
}
