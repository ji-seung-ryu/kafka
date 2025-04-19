package kafka.examples.car;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiLogProducer {
    private static final String TOPIC = "topic_log";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        int vehicleCount = 5; // 5대 차량
        ExecutorService executor = Executors.newFixedThreadPool(vehicleCount);

        for (int i = 1; i <= vehicleCount; i++) {
            int vehicleId = i;
            executor.execute(() -> startVehicleLogger(vehicleId));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            System.out.println("🛑 All vehicle log producers stopped.");
        }));
    }

    private static void startVehicleLogger(int vehicleId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        Producer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        try {
            while (true) {
                String route = "Route-" + (random.nextInt(10) + 1);
                double fuelLevel = random.nextDouble() * 100;
                String errorCode = fuelLevel < 15 ? "LOW_FUEL" : "OK"; // 15% 미만이면 LOW_FUEL
                long timestamp = System.currentTimeMillis();

                String message = String.format(
                        "{\"vehicle_id\": %d, \"route\": \"%s\", \"fuel_level\": %.2f, \"error_code\": \"%s\", \"timestamp\": %d}",
                        vehicleId, route, fuelLevel, errorCode, timestamp
                );

                System.out.printf("📋 [Log Vehicle %d] Sending: %s%n", vehicleId, message);
                producer.send(new ProducerRecord<>(TOPIC, String.valueOf(vehicleId), message));

                Thread.sleep(1000 + random.nextInt(1000)); // 1~2초 간격 로그 전송
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
            System.out.println("❌ [Log Vehicle " + vehicleId + "] stopped.");
        }
    }
}
