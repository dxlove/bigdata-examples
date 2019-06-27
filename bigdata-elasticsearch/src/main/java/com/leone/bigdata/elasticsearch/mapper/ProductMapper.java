package com.leone.bigdata.elasticsearch.mapper;

import com.leone.bigdata.elasticsearch.entity.Product;
import org.apache.ibatis.annotations.Mapper;
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

}
