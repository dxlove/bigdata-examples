package com.leone.bigdata.log.util;

import com.leone.bigdata.common.util.RandomValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * <p>
 *
 * @author leone
 * @since 2019-04-01
 **/
public abstract class OrcUtil {

    private static final Logger logger = LoggerFactory.getLogger(OrcUtil.class);

    public static void main(String[] args) throws Exception {
        orcWriter(10000, "file:///root//logs//orc//test.orc");
    }

    public static void orcWriter(int count, String outputPath) throws IOException {
        TypeDescription schema = TypeDescription.createStruct()
                .addField("user_id", TypeDescription.createLong())
                .addField("username", TypeDescription.createString())
                .addField("age", TypeDescription.createInt())
                .addField("sex", TypeDescription.createString())
                .addField("deleted", TypeDescription.createBoolean())
                .addField("create_time", TypeDescription.createTimestamp());
        // 输出ORC文件本地绝对路径
        Configuration conf = new Configuration();
        FileSystem.getLocal(conf);
        Writer writer = OrcFile.createWriter(new Path(outputPath),
                OrcFile.writerOptions(conf)
                        .setSchema(schema)
                        .stripeSize(67108864)
                        .bufferSize(131072)
                        .blockSize(134217728)
                        .compress(CompressionKind.ZLIB)
                        .version(OrcFile.Version.V_0_12));
        VectorizedRowBatch batch = schema.createRowBatch();

        for (int i = 0; i < count; i++) {
            int rowCount = batch.size++;
            ((LongColumnVector) batch.cols[i]).vector[0] = RandomValue.randomLong();
            ((BytesColumnVector) batch.cols[i]).vector[1] = RandomValue.randomUsername().getBytes(StandardCharsets.UTF_8);
            ((LongColumnVector) batch.cols[i]).vector[2] = RandomValue.randomLong();
            ((BytesColumnVector) batch.cols[i]).vector[3] = (RandomValue.RANDOM.nextBoolean() ? "男" : "女").getBytes(StandardCharsets.UTF_8);
            ((BytesColumnVector) batch.cols[i]).vector[4] = String.valueOf(RandomValue.RANDOM.nextBoolean()).getBytes(StandardCharsets.UTF_8);
            ((BytesColumnVector) batch.cols[i]).vector[6] = RandomValue.randomTime().getBytes(StandardCharsets.UTF_8);
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }

        logger.info("writer orc file {} successful...", outputPath);
    }

}
