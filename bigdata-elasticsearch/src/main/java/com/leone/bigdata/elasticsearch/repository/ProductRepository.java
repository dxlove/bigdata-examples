package com.leone.bigdata.elasticsearch.repository;

import com.leone.bigdata.elasticsearch.entity.Product;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-27
 **/
@Repository
public interface ProductRepository extends ElasticsearchRepository<Product, Long> {

    /**
     * 搜索查询
     *
     * @param productName  商品名称
     * @param productTitle 商品标题
     * @param keywords     商品关键字
     * @param page         分页信息
     * @return
     */
    Page<Product> findByProductNameAndProductTitleAndKeywords(String productName, String productTitle, String keywords, Pageable page);

}
