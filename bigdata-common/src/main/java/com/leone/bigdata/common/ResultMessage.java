package com.leone.bigdata.common;

/**
 * @author leone
 **/
public enum ResultMessage {

    PERMISSION_DENIED(40001, "permission.error"),
    SERVER_ERROR(40012, "server.error"),
    ;

    private Integer code;

    private String message;

    ResultMessage(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

    ResultMessage() {
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
