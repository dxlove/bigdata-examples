package com.leone.bigdata.elasticsearch.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.io.Serializable;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-27
 **/
@Document(indexName = "store", type = "product", shards = 1, replicas = 0)
public class EsProduct implements Serializable {
    private static final long serialVersionUID = -1L;

    @Id
    private Long productId;

    // 商品分类id
    private Integer categoryId;

    // 商品分类名称
    //@Field(type = FieldType.Keyword)
    private String categoryName;

    // 商品标题
    //@Field(analyzer = "ik_max_word", type = FieldType.Text)
    private String productTitle;

    // 商品名称
    //@Field(analyzer = "ik_max_word", type = FieldType.Text)
    private String productName;

    // 商品价格
    private Long productPrice;

    // 搜索关键字
    //@Field(analyzer = "ik_max_word", type = FieldType.Text)
    private String keywords;

    // 商品序列号
    //@Field(type = FieldType.Keyword)
    private String productSn;

    // 商标名称
    //@Field(type = FieldType.Keyword)
    private String brandName;

    // 商品销量
    private Integer productSale;

    // 商品库存
    private Integer stock;

    private String description;

    public EsProduct(Long productId) {
        this.productId = productId;
    }

    public EsProduct() {
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public String getProductTitle() {
        return productTitle;
    }

    public void setProductTitle(String productTitle) {
        this.productTitle = productTitle;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Long getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(Long productPrice) {
        this.productPrice = productPrice;
    }

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public String getProductSn() {
        return productSn;
    }

    public void setProductSn(String productSn) {
        this.productSn = productSn;
    }

    public String getBrandName() {
        return brandName;
    }

    public void setBrandName(String brandName) {
        this.brandName = brandName;
    }

    public Integer getProductSale() {
        return productSale;
    }

    public void setProductSale(Integer productSale) {
        this.productSale = productSale;
    }

    public Integer getStock() {
        return stock;
    }

    public void setStock(Integer stock) {
        this.stock = stock;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
