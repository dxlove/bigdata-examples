package com.leone.bigdata.log.util;

import com.leone.bigdata.common.util.RandomValue;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-21
 **/
public class ParquetUtil {

    private static Logger logger = LoggerFactory.getLogger(ParquetUtil.class);

    private static String schemaStr = "message schema {"
            + "optional int64 userId;"
            + "optional binary username (UTF8);"
            + "optional int32 sex;"
            + "optional int32 age;"
            + "optional double credit;"
            + "optional binary createTime (UTF8);"
            + "optional boolean deleted;}";


    private static MessageType schema = MessageTypeParser.parseMessageType(schemaStr);

    public static void main(String[] args) throws IOException {
        //String inputPath = "file:///root/logs/user.parquet";
        //String outputPath = "file:///root/logs/user.parquet";
        //parquetWriter(100000L,outputPath);
    }

    private static long offset;

    /**
     * 生成parquet文件
     *
     * @throws IOException
     */
    public static void parquetWriter(Long count, String outputPath) throws IOException {
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(outputPath))
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withType(schema);
        ParquetWriter<Group> writer = builder.build();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        for (long i = 0; i < count; i++) {
            writer.write(groupFactory.newGroup()
                    .append("userId", offset++)
                    .append("username", RandomValue.randomUsername())
                    .append("sex", RandomValue.RANDOM.nextInt(2))
                    .append("age", (RandomValue.randomInt(80)))
                    .append("credit", RandomValue.randomDouble(100))
                    .append("deleted", RandomValue.RANDOM.nextBoolean())
                    .append("createTime", RandomValue.randomTime()));

        }
        logger.info("save parquet file {} successful...", outputPath);
        writer.close();
    }

    /**
     * 读取parquet文件
     *
     * @throws IOException
     */
    private static void parquetReader(String inputPath) throws IOException {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = new ParquetReader<>(new Path(inputPath), readSupport);
        Group line;
        while ((line = reader.read()) != null) {
            System.out.println(line.toString());
        }
        reader.close();
    }

}
