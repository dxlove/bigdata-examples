package com.leone.bigdata.elasticsearch.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.io.Serializable;
import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-27
 **/
@Document(indexName = "store", type = "product", shards = 1, replicas = 0)
public class Product implements Serializable {
    private static final long serialVersionUID = -1L;

    @Id
    private Long productId;

    // 商品分类id
    private Long productCategoryId;

    // 商品分类名称
    //@Field(type = FieldType.Keyword)
    private String productCategoryName;

    // 商品序列号
    //@Field(type = FieldType.Keyword)
    private String productSn;

    // 商标id
    private Long brandId;


    // 商标名称
    //@Field(type = FieldType.Keyword)
    private String brandName;

    // 商品图片
    private String picture;

    // 商品名称
    //@Field(analyzer = "ik_max_word", type = FieldType.Text)
    private String productName;

    // 商品标题
    //@Field(analyzer = "ik_max_word", type = FieldType.Text)
    private String productTitle;

    // 搜索关键字
    //@Field(analyzer = "ik_max_word", type = FieldType.Text)
    private String keywords;

    // 商品价格
    private Integer productPrice;

    // 商品销量
    private Integer productSale;

    // 商品状态
    private Integer productStatus;

    // 商品库存
    private Integer stock;

    // 商品属性集合
    //@Field(type = FieldType.Nested)
    private List<ProductAttribute> attributeList;


    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Long getProductCategoryId() {
        return productCategoryId;
    }

    public void setProductCategoryId(Long productCategoryId) {
        this.productCategoryId = productCategoryId;
    }

    public String getProductCategoryName() {
        return productCategoryName;
    }

    public void setProductCategoryName(String productCategoryName) {
        this.productCategoryName = productCategoryName;
    }

    public String getProductSn() {
        return productSn;
    }

    public void setProductSn(String productSn) {
        this.productSn = productSn;
    }

    public Long getBrandId() {
        return brandId;
    }

    public void setBrandId(Long brandId) {
        this.brandId = brandId;
    }

    public String getBrandName() {
        return brandName;
    }

    public void setBrandName(String brandName) {
        this.brandName = brandName;
    }

    public String getPicture() {
        return picture;
    }

    public void setPicture(String picture) {
        this.picture = picture;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductTitle() {
        return productTitle;
    }

    public void setProductTitle(String productTitle) {
        this.productTitle = productTitle;
    }

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public Integer getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(Integer productPrice) {
        this.productPrice = productPrice;
    }

    public Integer getProductSale() {
        return productSale;
    }

    public void setProductSale(Integer productSale) {
        this.productSale = productSale;
    }

    public Integer getProductStatus() {
        return productStatus;
    }

    public void setProductStatus(Integer productStatus) {
        this.productStatus = productStatus;
    }

    public Integer getStock() {
        return stock;
    }

    public void setStock(Integer stock) {
        this.stock = stock;
    }

    public List<ProductAttribute> getAttributeList() {
        return attributeList;
    }

    public void setAttributeList(List<ProductAttribute> attributeList) {
        this.attributeList = attributeList;
    }
}
