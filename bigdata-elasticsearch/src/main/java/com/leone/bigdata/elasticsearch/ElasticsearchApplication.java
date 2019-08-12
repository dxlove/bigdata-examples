package com.leone.bigdata.elasticsearch;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-27
 **/
@SpringBootApplication
@MapperScan("com.leone.bigdata.elasticsearch.mapper")
public class ElasticsearchApplication {
    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchApplication.class, args);
    }
}
