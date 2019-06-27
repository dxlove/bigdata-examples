package com.leone.bigdata.elasticsearch.service;

import com.leone.bigdata.common.util.RandomValue;
import com.leone.bigdata.elasticsearch.entity.EsProduct;
import com.leone.bigdata.elasticsearch.entity.Product;
import com.leone.bigdata.elasticsearch.mapper.ProductMapper;
import com.leone.bigdata.elasticsearch.repository.ProductRepository;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

    @Autowired
    private ProductRepository productRepository;

    /**
     * 从mysql数据库中加载所有商品数据
     *
     * @return
     */
    public List<Product> findAllByMySql() {
        return productMapper.findAll();
    }

    /**
     * 从mysql中导入到ES
     *
     * @return
     */
    public Integer importAll() {
        List<Product> productList = findAllByMySql();
        List<EsProduct> collect = productList.stream().map(e -> {
            EsProduct esProduct = new EsProduct();
            BeanUtils.copyProperties(e, esProduct);
            return esProduct;
        }).collect(Collectors.toList());

        Iterable<EsProduct> products = productRepository.saveAll(collect);
        int count = 0;
        for (EsProduct product : products) {
            count++;
        }
        return count;
    }


    /**
     * 从ES删除商品
     *
     * @param productId
     * @return
     */
    public Integer deleteFromEs(Long productId) {
        try {
            productRepository.deleteById(productId);
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
        return 1;
    }

    /**
     * 批量删除
     *
     * @param productIds
     * @return
     */
    public Integer deleteBatchFromEs(Set<Long> productIds) {
        if (!CollectionUtils.isEmpty(productIds)) {
            final int[] arr = {0};
            Set<EsProduct> ESProductSet = new HashSet<>();
            productIds.forEach(e -> {
                arr[0]++;
                ESProductSet.add(new EsProduct(e));
            });
            productRepository.deleteAll(ESProductSet);
            return arr[0];
        }
        return 0;
    }

    /**
     * 分页查找
     *
     * @param keyword
     * @param pageable
     * @return
     */
    public Page<EsProduct> search(String keyword, Pageable pageable) {
        return productRepository.findByProductNameAndProductTitleAndKeywords(keyword, keyword, keyword, pageable);
    }


    /**
     * 根据商品id创建商品
     *
     * @param productId
     * @return
     */
    public EsProduct create(Long productId) {
        Product product = productMapper.findOne(productId);
        if (ObjectUtils.isEmpty(product)) {
            EsProduct esProduct = new EsProduct();
            BeanUtils.copyProperties(product, esProduct);
            return productRepository.save(esProduct);
        }
        return null;
    }


    /**
     * @param count
     * @return
     */
    public Integer insert(Integer count) {
        for (int i = 0; i < count; i++) {
            Product product = new Product();
            product.setBrandName(RandomValue.randomBrandName());
            product.setProductName(RandomValue.randomBrandName() + "手机");
            int result = productMapper.insert(product);
        }
        return null;
    }

}
