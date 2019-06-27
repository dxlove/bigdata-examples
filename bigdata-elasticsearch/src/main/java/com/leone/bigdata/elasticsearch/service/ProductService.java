package com.leone.bigdata.elasticsearch.service;

import com.leone.bigdata.elasticsearch.entity.Product;
import com.leone.bigdata.elasticsearch.mapper.ProductMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-27
 **/
@Service
public class ProductService {

    @Autowired
    private ProductMapper productMapper;

    /**
     * 从mysql数据库中加载所有商品数据
     *
     * @return
     */
    public List<Product> findAllByMySql() {
        return productMapper.findAll();
    }


}
