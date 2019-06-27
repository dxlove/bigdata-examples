package com.leone.bigdata.elasticsearch.controller;

import com.leone.bigdata.common.Result;
import com.leone.bigdata.elasticsearch.entity.EsProduct;
import com.leone.bigdata.elasticsearch.entity.Product;
import com.leone.bigdata.elasticsearch.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Set;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-27
 **/
@RestController
@RequestMapping("/api/product")
public class ProductController {

    @Autowired
    private ProductService productService;

    @GetMapping("/list")
    public List<Product> findAll() {
        return productService.findAllByMySql();
    }

    @GetMapping("insert")
    public Result<Integer> insert(Integer count) {
        return Result.success(productService.insert(count));
    }

    /**
     * 从数据库导入到ES
     *
     * @return
     */
    @GetMapping("/importAll")
    public Result<Integer> importAllList() {
        return Result.success(productService.importAll());
    }

    @DeleteMapping
    public Result deleteFromEs(@RequestParam Long productId) {
        return Result.success(productService.deleteFromEs(productId));
    }

    @DeleteMapping("/batch")
    public Result deleteBathFromEs(@RequestParam Set<Long> productIds) {
        return Result.success(productService.deleteBatchFromEs(productIds));
    }

    /**
     * 模糊搜索
     *
     * @param query
     * @param pageable
     * @return
     */
    @GetMapping("/page")
    public Result<Page<EsProduct>> search(@RequestParam String query, @PageableDefault Pageable pageable) {
        return Result.success(productService.search(query, pageable));
    }

    @PostMapping("/{productId}")
    public Result<EsProduct> create(@PathVariable Long productId) {
        return Result.success(productService.create(productId));
    }


}
