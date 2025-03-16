package kafka.examples.jiseung;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class PriorityProducer {
    public static void main(String[] args) {
        String topicName = "example-topic";

        // Kafka Producer 설정
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        // 10개의 메시지 생성
        for (int i = 1; i<=1; i++) {
            String key = "key-" + i;
            String value = "message-" + i;
            producer.send(new ProducerRecord<>(topicName, key, value));
            System.out.printf("Produced record: (key=%s, value=%s)%n", key, value);
        }

        producer.close();
    }
}
