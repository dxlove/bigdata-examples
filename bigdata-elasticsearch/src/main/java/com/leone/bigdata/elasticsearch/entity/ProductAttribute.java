package com.leone.bigdata.elasticsearch.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.io.Serializable;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-27
 **/
public class ProductAttribute implements Serializable {
    private static final long serialVersionUID = 1L;

    // 主键
    @Id
    private Long productAttributeId;

    // 属性名称
    @Field(type = FieldType.Keyword)
    private String name;

    // 属性值
    @Field(type = FieldType.Keyword)
    private String value;

    // 属性参数：0->规格；1->参数
    private Integer type;

    public Long getProductAttributeId() {
        return productAttributeId;
    }

    public void setProductAttributeId(Long productAttributeId) {
        this.productAttributeId = productAttributeId;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
