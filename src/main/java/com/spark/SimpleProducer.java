package com.spark;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Gửi 100 message
        for (int i = 0; i < 100; i++) {
            String message = "Message " + i + " at " + System.currentTimeMillis();
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("test-topic", "key" + i, message);

            producer.send(record);
            System.out.println("Sent: " + message);

            Thread.sleep(1000); // 1 giây 1 message
        }

        producer.close();
    }
}