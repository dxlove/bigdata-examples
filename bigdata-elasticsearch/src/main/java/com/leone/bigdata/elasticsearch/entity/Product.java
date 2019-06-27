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
    @Field(type = FieldType.Keyword)
    private String productCategoryName;

    // 商品序列号
    @Field(type = FieldType.Keyword)
    private String productSn;

    // 商标id
    private Long brandId;
    @Field(type = FieldType.Keyword)

    // 商标名称
    private String brandName;

    // 商品图片
    private String picture;

    // 商品名称
    @Field(analyzer = "ik_max_word", type = FieldType.Text)
    private String productName;

    // 商品标题
    @Field(analyzer = "ik_max_word", type = FieldType.Text)
    private String productTitle;

    // 搜索关键字
    @Field(analyzer = "ik_max_word", type = FieldType.Text)
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
    @Field(type = FieldType.Nested)
    private List<ProductAttribute> attributeList;

}
