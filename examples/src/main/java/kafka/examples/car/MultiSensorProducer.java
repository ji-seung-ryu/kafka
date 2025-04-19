package kafka.examples.car;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiSensorProducer {

    private static final String TOPIC = "topic_sensor";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        int vehicleCount = 5; // 5대 차량
        ExecutorService executor = Executors.newFixedThreadPool(vehicleCount);

        for (int i = 1; i <= vehicleCount; i++) {
            int vehicleId = i;
            executor.execute(() -> startVehicleSimulator(vehicleId));
        }

        // 안전한 종료 처리
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            System.out.println("🚗 All vehicle producers stopped.");
        }));
    }

    private static void startVehicleSimulator(int vehicleId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        Producer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        try {
            double speed = 50.0; // 초기 속도
            while (true) {
                // 점진적으로 속도/거리 변화
                speed += random.nextGaussian(); // 자연스러운 움직임
                speed = Math.max(0, Math.min(150, speed));

                double distance = 1 + (49 * random.nextDouble());
                boolean obstacle = random.nextDouble() < 0.05; // 5% 확률로 장애물
                long timestamp = System.currentTimeMillis();

                String message = String.format(
                        "{\"vehicle_id\": %d, \"speed\": %.2f, \"distance\": %.2f, \"obstacle\": %b, \"timestamp\": %d}",
                        vehicleId, speed, distance, obstacle, timestamp
                );

                System.out.printf("🚗 [Vehicle %d] Sending: %s%n", vehicleId, message);
                producer.send(new ProducerRecord<>(TOPIC, String.valueOf(vehicleId), message));

                Thread.sleep(20000 + random.nextInt(100)); // 200~300ms 간격
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
            System.out.println("❌ [Vehicle " + vehicleId + "] stopped.");
        }
    }
}
