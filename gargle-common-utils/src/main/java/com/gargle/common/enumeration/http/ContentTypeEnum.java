package com.gargle.common.enumeration.http;

/**
 * ClassName:ContentTypeEnum
 * Description:
 *
 * @author qingwen.shang
 */
public enum ContentTypeEnum {

    APPLICATION_X_WWW_FORM_URLENCODED("application/x-www-form-urlencoded"),
    APPLICATION_JSON("application/json"),
    ;

    private String code;

    ContentTypeEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
