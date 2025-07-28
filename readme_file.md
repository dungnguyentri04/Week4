# Real-time Data Pipeline với Apache Kafka và Spark Streaming



### 1. Cài đặt và chạy Apache Kafka

```bash

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Server 
bin/kafka-server-start.sh config/server.properties
```

### 2. Tạo Kafka Topic

```bash
bin/kafka-topics.sh --create --topic test-topic  --bootstrap-server localhost:9092  --partitions 1 --replication-factor 1

# danh sách topic đã tạo 
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 3. Chạy project

```bash

mvn clean compile
```
Chạy file com.spark.SimpleProducer
Chạy file com.spark.SimpleStreaming


## Kết quả 

### Producer Output:
```
Sent: Message 0 at 1753692520787
Sent: Message 1 at 1753692522189
Sent: Message 2 at 1753692523194
Sent: Message 3 at 1753692524196
Sent: Message 4 at 1753692525201
...
```

### Streaming Consumer Output:
```
-------------------------------------------
Batch: 0
-------------------------------------------
+---+-----+
|key|count|
+---+-----+
+---+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----+-----+
|  key|count|
+-----+-----+
|key24|    1|
|key30|    1|
|key18|    1|
|key28|    1|
|key35|    1|
|key31|    1|
|key34|    1|
|key17|    1|
|key16|    1|
|key32|    1|
|key21|    1|
|key22|    1|
```

## Code

### 1. SimpleProducer.java - Kafka Producer

Tạo ra một Kafka Producer để gửi dữ liệu test vào Kafka topic.


```java
// 1. Cấu hình Producer
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");          // Địa chỉ Kafka server
props.put("key.serializer", StringSerializer.class.getName());   // Serializer cho key
props.put("value.serializer", StringSerializer.class.getName()); // Serializer cho value
```

```java
// 2. Gửi message trong vòng lặp
for (int i = 0; i < 100; i++) {
    String message = "Message " + i + " at " + System.currentTimeMillis();
    ProducerRecord<String, String> record = 
        new ProducerRecord<>("test-topic", "key" + i, message);
    
    producer.send(record);
}
```

### 2. SimpleStreaming.java - Spark Streaming Consumer

Tạo Spark Streaming application để đọc dữ liệu từ Kafka và xử lý real-time.


```java
// 1. Tạo SparkSession
SparkSession spark = SparkSession.builder()
    .appName("SimpleKafkaStreaming")   
    .master("local[2]")               
    .getOrCreate();
```

```java
// 2. Tạo streaming DataFrame từ Kafka
Dataset<Row> df = spark
    .readStream()                                           // Tạo streaming read
    .format("kafka")                                        // Source là Kafka
    .option("kafka.bootstrap.servers", "localhost:9092")   // Kafka server address
    .option("subscribe", "test-topic")                      // Topic để subscribe
    .load();
```


```java
// 3. Transform dữ liệu
Dataset<Row> messages = df.select(
    col("key").cast("string"),      
    col("value").cast("string"),    
    col("timestamp")                
);
```
