package com.leone.bigdata.common.entity;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-22
 **/
public class Http {

    private Integer http_id;
    private String ip;
    private Integer port;
    private String protocol;
    private String anonymity;
    private String country;
    private Integer timeout;
    private Integer enable;

    public Integer getHttp_id() {
        return http_id;
    }

    public void setHttp_id(Integer http_id) {
        this.http_id = http_id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getAnonymity() {
        return anonymity;
    }

    public void setAnonymity(String anonymity) {
        this.anonymity = anonymity;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Integer getEnable() {
        return enable;
    }

    public void setEnable(Integer enable) {
        this.enable = enable;
    }
}
