package com.leone.bigdata.spark.java.bean;

import java.io.Serializable;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-20
 **/
public class Student implements Serializable {

    private Integer studentId;

    private String name;

    private Integer age;

    private Integer sex;

    private Integer score;

    private Integer course;

    public Student() {
    }

    public Student(Integer studentId, String name, Integer age, Integer sex, Integer score, Integer course) {
        this.studentId = studentId;
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.score = score;
        this.course = course;
    }

    public Integer getStudentId() {
        return studentId;
    }

    public void setStudentId(Integer studentId) {
        this.studentId = studentId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getSex() {
        return sex;
    }

    public void setSex(Integer sex) {
        this.sex = sex;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public Integer getCourse() {
        return course;
    }

    public void setCourse(Integer course) {
        this.course = course;
    }
}