package com.leone.bigdata.common.entity;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-22
 **/
public class Ss {

    private Integer ss_id;
    private String ip;
    private Integer port;
    private String password;
    private String encrypt;
    private String country;
    private Integer enable;
    private Integer timeout;

    public Integer getSs_id() {
        return ss_id;
    }

    public void setSs_id(Integer ss_id) {
        this.ss_id = ss_id;
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

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getEncrypt() {
        return encrypt;
    }

    public void setEncrypt(String encrypt) {
        this.encrypt = encrypt;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Integer getEnable() {
        return enable;
    }

    public void setEnable(Integer enable) {
        this.enable = enable;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }
}
