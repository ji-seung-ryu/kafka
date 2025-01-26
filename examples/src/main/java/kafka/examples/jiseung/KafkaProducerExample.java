package kafka.examples.jiseung;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaProducerExample {
    public static void main(String[] args) {
        String topicName = "example-topic";

        // Kafka 프로듀서 설정
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // 메시지 전송
            for (int i = 0; i < 10; i++) {
                String key = "key-" + i;
                String value = "value-" + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

                // 동기적으로 전송
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("Sent record(key=%s value=%s) meta(partition=%d, offset=%d)%n",
                        key, value, metadata.partition(), metadata.offset());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}