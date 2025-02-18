package com.leone.bigdata.hadoop.mr.weblog;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-07
 **/
public class WebLogBean {

    // 记录客户端的ip地址
    private String remote_addr;

    // 记录客户端用户名称,忽略属性"-"
    private String remote_user;

    // 记录访问时间与时区
    private String time_local;

    // 记录请求的url与http协议
    private String request;

    // 记录请求状态；成功是200
    private String status;

    // 记录发送给客户端文件主体内容大小
    private String body_bytes_sent;

    // 用来记录从那个页面链接访问过来的
    private String http_referer;

    // 记录客户浏览器的相关信息
    private String http_user_agent;

    // 判断数据是否合法
    private boolean valid = true;

    public void set(String remote_addr, String remote_user, String time_local, String request, String status, String body_bytes_sent, String http_referer) {
        this.remote_addr = remote_addr;
        this.remote_user = remote_user;
        this.time_local = time_local;
        this.request = request;
        this.status = status;
        this.body_bytes_sent = body_bytes_sent;
        this.http_referer = http_referer;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
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

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.valid);
        sb.append("\t").append(this.remote_addr);
        sb.append("\t").append(this.remote_user);
        sb.append("\t").append(this.time_local);
        sb.append("\t").append(this.request);
        sb.append("\t").append(this.status);
        sb.append("\t").append(this.body_bytes_sent);
        sb.append("\t").append(this.http_referer);
        sb.append("\t").append(this.http_user_agent);
        return sb.toString();
    }


}
