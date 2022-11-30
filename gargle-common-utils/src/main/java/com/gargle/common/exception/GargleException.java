package com.gargle.common.exception;

/**
 * ClassName:GargleException
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/11/30 10:42
 */
public class GargleException extends RuntimeException {

    public GargleException() {
    }

    public GargleException(String message) {
        super(message);
    }

    public GargleException(String messageFormat, Object... objects) {
        super(String.format(messageFormat, objects));
    }

    public GargleException(String message, Throwable cause) {
        super(message, cause);
    }

    public GargleException(Throwable cause, String messageFormat, Object... objects) {
        super(String.format(messageFormat, objects), cause);
    }

    public GargleException(Throwable cause) {
        super(cause);
    }

    public GargleException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }


}
