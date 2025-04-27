package kafka.examples.car;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class LogProducer {
    private static final String TOPIC = "topic_log";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // 빠른 처리를 위해 1로 설정

        Producer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        try {
            while (true) {
                int vehicleId = random.nextInt(100) + 1;
                String route = "Route-" + (random.nextInt(10) + 1);
                double fuelLevel = random.nextDouble() * 100;
                String errorCode = random.nextBoolean() ? "OK" : "LOW_FUEL";
                long timestamp = System.currentTimeMillis();

                String message = String.format("{\"vehicle_id\": %d, \"route\": \"%s\", \"fuel_level\": %.2f, \"error_code\": \"%s\", \"timestamp\": %d}",
                        vehicleId, route, fuelLevel, errorCode, timestamp);

                System.out.println("[Log Producer] Sending: " + message);
                producer.send(new ProducerRecord<>(TOPIC, String.valueOf(vehicleId), message));

                Thread.sleep(10000); // 10초 간격으로 로그 데이터 전송
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
