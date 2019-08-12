package com.leone.bigdata.spark.java.examples.nginxAccessLog;

import java.io.Serializable;

/**
 * <p>
 *
 * @author leone
 * @since 2019-04-24
 **/
public class LogBean implements Serializable {

    private static final long serialVersionUID = 8503828825924680771L;

    private String remote_addr;

    private String extra;

    private String remote_user;

    private String time_local;

    private String request;

    private String status;

    private String body_bytes_sent;

    private String http_referer;

    private String http_user_agent;

    private String http_x_forwarded_for;

    public LogBean() {
    }

    public LogBean(String remote_addr, String extra, String remote_user, String time_local, String request, String status, String body_bytes_sent, String http_referer, String http_user_agent, String http_x_forwarded_for) {
        this.remote_addr = remote_addr;
        this.extra = extra;
        this.remote_user = remote_user;
        this.time_local = time_local;
        this.request = request;
        this.status = status;
        this.body_bytes_sent = body_bytes_sent;
        this.http_referer = http_referer;
        this.http_user_agent = http_user_agent;
        this.http_x_forwarded_for = http_x_forwarded_for;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public String getHttp_x_forwarded_for() {
        return http_x_forwarded_for;
    }

    public void setHttp_x_forwarded_for(String http_x_forwarded_for) {
        this.http_x_forwarded_for = http_x_forwarded_for;
    }
}
