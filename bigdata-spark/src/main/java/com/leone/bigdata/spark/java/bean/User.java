package com.leone.bigdata.spark.java.bean;

import java.io.Serializable;

public class User implements Serializable {

    private Long userId;

    private String username;

    private Integer sex;

    private Integer age;

    private Double credit;

    private String createTime;

    private boolean deleted;

    public User() {
    }

    public User(Long userId, String username, Integer sex, Integer age, Double credit, String createTime, boolean deleted) {
        this.userId = userId;
        this.username = username;
        this.sex = sex;
        this.age = age;
        this.credit = credit;
        this.createTime = createTime;
        this.deleted = deleted;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Integer getSex() {
        return sex;
    }

    public void setSex(Integer sex) {
        this.sex = sex;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Double getCredit() {
        return credit;
    }

    public void setCredit(Double credit) {
        this.credit = credit;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
}