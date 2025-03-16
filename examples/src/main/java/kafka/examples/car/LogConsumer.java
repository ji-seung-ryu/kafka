package kafka.examples.car;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LogConsumer {
    private static final String TOPIC = "topic_log";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "log_group";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("priority", 5);

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("[Log Consumer] Waiting for sensor data...");

        Map<String, Long> lastTimestampMap = new HashMap<>();

        while (true) {
            long startTime = System.currentTimeMillis();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String vehicleId = record.key();
                long timestamp = Long.parseLong(record.value().split("\"timestamp\": ")[1].replace("}", "")); // timestamp 추출

                // 이전 메시지보다 timestamp가 더 작다면, 순서가 어긋난 것!
                if (lastTimestampMap.containsKey(vehicleId) && lastTimestampMap.get(vehicleId) > timestamp) {
                    System.err.println("❌ [ERROR] Message order broken for Vehicle " + vehicleId);
                } else {
                    System.out.println("✅ [Consumer] Processing: " + record.value());
                    long processingTime = System.currentTimeMillis() - startTime;
                    System.out.println("✅ [Log Consumer] Processing Time: " + processingTime + "ms\n");
                    lastTimestampMap.put(vehicleId, timestamp); // 최신 timestamp 업데이트
                }
            }
        }
    }
}
