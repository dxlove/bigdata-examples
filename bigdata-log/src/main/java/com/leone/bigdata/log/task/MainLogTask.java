package com.leone.bigdata.log.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leone.bigdata.common.util.RandomValue;
import com.leone.bigdata.log.kafka.KafkaSender;
import com.leone.bigdata.log.util.OrcUtil;
import com.leone.bigdata.log.util.ParquetUtil;
import com.leone.bigdata.common.util.RandomValue;
import org.apache.orc.OrcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Random;

/**
 * 模拟用户产生日志
 * 每隔指定毫秒生成日志   @Scheduled(fixedRate = 100)
 * 使用cron表达式       @Scheduled(cron = "0/1 * * * * ?")
 *
 * @author leone
 * @since 2018-06-12
 **/
@Component
public class MainLogTask {

    @Autowired
    private KafkaSender kafkaSender;

    private static final Logger JSON_LOG = LoggerFactory.getLogger("json-log");

    private static final Logger CSV_LOG = LoggerFactory.getLogger("csv-log");

    private static final Logger COMMON_LOG = LoggerFactory.getLogger("common-log");

    private static final Random RANDOM = new Random();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static Long offset = 0L;

    private static LocalDateTime localDateTime = LocalDateTime.now();

    /**
     * 产生 csv 日志
     */
    @Async
    @Scheduled(fixedRate = 5)
    public void csvLogTask() {
        CSV_LOG.info(offset++ + "," + RandomValue.randomUsername() + "," + RandomValue.randomDateTime() + "," + RandomValue.RANDOM.nextBoolean() + "," + RandomValue.randomStr(12) + "," + RandomValue.randomInt(70) + "," + RandomValue.randomWords());
    }

    /**
     * 产生 json 日志任务
     */
    @Async
    @Scheduled(fixedDelay = 5)
    public void jsonLogTask() throws JsonProcessingException {
        JSON_LOG.info(objectMapper.writeValueAsString(RandomValue.randomUser()));
    }

    /**
     * 基本日志 task
     */
    @Async
    //@Scheduled(fixedDelay = 5)
    public void commonLogTask() {
        COMMON_LOG.info(RandomValue.randomInt(1000000000) + "," + RandomValue.randomUsername() + "," + RandomValue.randomInt(80) + "," + RandomValue.randomDouble(100));
    }

    /**
     * 向 kafka 发送数据
     */
    @Async
    //@Scheduled(fixedDelay = 500)
    public void kafkaSenderTask() {
        kafkaSender.send("topic-kafka-streaming", RandomValue.randomWords() + " " + RandomValue.randomWords() + " " + RandomValue.randomWords() + " " + RandomValue.randomWords());
        offset++;
    }

    /**
     * 产生 parquet 文件
     */
    @Async
    //@Scheduled(cron = "0/15 * * * * ?")
    public void parquetTask() throws IOException {
        ParquetUtil.parquetWriter(100000L, "file:///root/logs/parquet/user-" + RandomValue.currentTimestampStr() + ".parquet");
    }

    /**
     * 产生 orc 文件
     */
    @Async
    //@Scheduled(cron = "0/10 * * * * ?")
    public void orcTask() throws IOException {
        OrcUtil.orcWriter(100000, "file:///root/logs/orc/user-" + RandomValue.currentTimestampStr() + ".orc");
    }

}
