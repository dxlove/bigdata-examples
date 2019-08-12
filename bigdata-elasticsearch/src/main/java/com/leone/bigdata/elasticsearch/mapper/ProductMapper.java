package com.leone.bigdata.elasticsearch.mapper;

import com.leone.bigdata.elasticsearch.entity.Product;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-27
 **/
@Mapper
public interface ProductMapper {

    @Select("select * from t_product")
    List<Product> findAll();

    @Select("select * from t_product where product_id = #{productId}")
    Product findOne(Long productId);

    @Insert("insert into t_product (`brand_id`, `brand_name`, `keywords`, `picture`, `product_category_id`, `product_category_name`, `product_name`, `product_price`, `product_sale`, `product_sn`, `product_status`, `product_title`, `stock`)" +
            " values (#{p.brandId}, #{p.brandName}, #{p.keywords}, #{p.picture}, #{p.productCategoryId}, #{p.productCategoryName}, #{p.productName}, #{p.productPrice}, #{p.productSale}, #{p.productSn}, #{p.productStatus}, #{p.productTitle}, #{p.stock})")
    int insert(@Param("p") Product product);

    int deleteByPrimaryKey(Long productId);

    int insertSelective(Product record);

    Product selectByPrimaryKey(Long productId);

    int updateByPrimaryKeySelective(Product record);

    int updateByPrimaryKey(Product record);

}
