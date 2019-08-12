package com.leone.bigdata.common.entity;

public class Ssr {

    private Integer ssr_id;
    private String ip;
    private Integer port;
    private String password;
    private String encrypt;
    private String protocol;
    private String confound;
    private String country;
    private Integer enable;

    public int getSsr_id() {
        return ssr_id;
    }

    public void setSsr_id(int ssr_id) {
        this.ssr_id = ssr_id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSsr_id(Integer ssr_id) {
        this.ssr_id = ssr_id;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getEncrypt() {
        return encrypt;
    }

    public void setEncrypt(String encrypt) {
        this.encrypt = encrypt;
    }

    public void setEnable(Integer enable) {
        this.enable = enable;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getConfound() {
        return confound;
    }

    public void setConfound(String confound) {
        this.confound = confound;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public int getEnable() {
        return enable;
    }

    public void setEnable(int enable) {
        this.enable = enable;
    }
}