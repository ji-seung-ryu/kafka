package kafka.examples.car;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class SensorProducer {
    private static final String TOPIC = "topic_sensor";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 메시지 손실 방지

        Producer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        try {
            while (true) {
                int vehicleId = random.nextInt(100) + 1;
                double speed = 20 + (100 * random.nextDouble());
                double distance = 1 + (49 * random.nextDouble());
                boolean obstacle = random.nextBoolean();
                long timestamp = System.currentTimeMillis();

                String message = String.format("{\"vehicle_id\": %d, \"speed\": %.2f, \"distance\": %.2f, \"obstacle\": %b, \"timestamp\": %d}",
                        vehicleId, speed, distance, obstacle, timestamp);

                System.out.println("[Sensor Producer] Sending: " + message);
                producer.send(new ProducerRecord<>(TOPIC, String.valueOf(vehicleId), message));

                Thread.sleep(50); // 50ms 간격으로 데이터 전송 (실시간)
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
