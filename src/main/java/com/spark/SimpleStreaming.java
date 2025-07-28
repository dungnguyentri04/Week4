package com.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import static org.apache.spark.sql.functions.*;

public class SimpleStreaming {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("SimpleKafkaStreaming")
                .master("local[2]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Đọc từ Kafka
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test-topic")
                .load();

        // Chuyển đổi data
        Dataset<Row> messages = df.select(
                col("key").cast("string"),
                col("value").cast("string"),
                col("timestamp")
        );

        // Đếm số message theo key
        Dataset<Row> wordCounts = messages
                .groupBy(col("key"))
                .count();

        // Xuất ra console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}