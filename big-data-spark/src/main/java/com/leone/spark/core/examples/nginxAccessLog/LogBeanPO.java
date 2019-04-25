package com.leone.spark.core.examples.nginxAccessLog;

import java.io.Serializable;

/**
 * <p>
 *
 * @author leone
 * @since 2019-04-24
 **/
public class LogBeanPO implements Serializable {

    private static final long serialVersionUID = -8088244852873380545L;

    private String remote_addr;

    private String address;

    private String time_local;

    private String request;

    private String req_method;

    private String path;

    private String http_deal;

    private String status;

    private String body_bytes_sent;

    private String http_user_agent;

    public LogBeanPO() {
    }

    public LogBeanPO(String remote_addr, String address, String time_local, String request, String req_method, String path, String http_deal, String status, String body_bytes_sent, String http_user_agent) {
        this.remote_addr = remote_addr;
        this.address = address;
        this.time_local = time_local;
        this.request = request;
        this.req_method = req_method;
        this.path = path;
        this.http_deal = http_deal;
        this.status = status;
        this.body_bytes_sent = body_bytes_sent;
        this.http_user_agent = http_user_agent;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getReq_method() {
        return req_method;
    }

    public void setReq_method(String req_method) {
        this.req_method = req_method;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getHttp_deal() {
        return http_deal;
    }

    public void setHttp_deal(String http_deal) {
        this.http_deal = http_deal;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
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

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    @Override
    public String toString() {
        return "LogBeanPO{" +
                "remote_addr='" + remote_addr + '\'' +
                ", address='" + address + '\'' +
                ", time_local='" + time_local + '\'' +
                ", request='" + request + '\'' +
                ", req_method='" + req_method + '\'' +
                ", path='" + path + '\'' +
                ", http_deal='" + http_deal + '\'' +
                ", status='" + status + '\'' +
                ", body_bytes_sent='" + body_bytes_sent + '\'' +
                ", http_user_agent='" + http_user_agent + '\'' +
                '}';
    }
}
