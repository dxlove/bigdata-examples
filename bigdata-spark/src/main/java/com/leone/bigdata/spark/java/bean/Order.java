package com.leone.bigdata.spark.java.bean;

import java.io.Serializable;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-25
 **/
public class Order implements Serializable {

    private Long orderId;

    private Long userId;

    private String productName;

    private Double productPrice;

    private Double totalAmount;

    private Integer productCount;

    private String createTime;

    public Order() {
    }

    public Order(Long orderId, Long userId, String productName, Double productPrice, Double totalAmount, Integer productCount, String createTime) {
        this.orderId = orderId;
        this.userId = userId;
        this.productName = productName;
        this.productPrice = productPrice;
        this.totalAmount = totalAmount;
        this.productCount = productCount;
        this.createTime = createTime;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Double getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(Double productPrice) {
        this.productPrice = productPrice;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Integer getProductCount() {
        return productCount;
    }

    public void setProductCount(Integer productCount) {
        this.productCount = productCount;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}


