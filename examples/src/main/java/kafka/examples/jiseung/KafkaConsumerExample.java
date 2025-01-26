package kafka.examples.jiseung;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {
    public static void main(String[] args) {
        String topicName = "example-topic";

        // Kafka 컨슈머 설정
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "example-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("priority", 2);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 토픽 구독
        consumer.subscribe(Collections.singletonList(topicName));

        try {
            while (true) {
                // 메시지 폴링
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    System.out.printf("Received record(key=%s value=%s partition=%d offset=%d)%n",
                            record.key(), record.value(), record.partition(), record.offset());
                });
            }
        } finally {
            consumer.close();
        }
    }
}
