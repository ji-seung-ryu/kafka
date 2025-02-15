package kafka.examples.jiseung;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class PriorityConsumer implements Runnable {
    private final String topic;
    private final int priority;
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch startSignal;

    public PriorityConsumer(String topic, int priority, CountDownLatch startSignal) {
        this.topic = topic;
        this.priority = priority;
        this.startSignal = startSignal;

        // Kafka Consumer 설정
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "example-group-" + priority); // 그룹 ID를 다르게 설정
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("priority", priority); // 브로커에서 처리할 우선순위

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        try {
            System.out.printf("Consumer(priority=%d) 준비 완료! 신호 대기 중...\n", priority);
            startSignal.await(); // 모든 컨슈머가 동시에 시작하도록 대기

            while (running.get()) {
                long startTime = System.currentTimeMillis();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                long endTime = System.currentTimeMillis();

                records.forEach(record -> {
                    System.out.printf("[FETCH] Consumer(priority=%d) received record(key=%s, value=%s, partition=%d, offset=%d) | Fetch Time: %d ms%n",
                            priority, record.key(), record.value(), record.partition(), record.offset(), (endTime - startTime));
                });
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            consumer.close();
            System.out.printf("Consumer(priority=%d) 종료됨.\n", priority);
        }
    }

    public void shutdown() {
        running.set(false);
    }

    public static void main(String[] args) {
        String topicName = "example-topic";
        int numConsumers = 5;
        ExecutorService executorService = Executors.newFixedThreadPool(numConsumers);
        CountDownLatch startSignal = new CountDownLatch(1); // 동기 시작 신호

        PriorityConsumer[] consumers = new PriorityConsumer[numConsumers];
        for (int i = 0; i < numConsumers; i++) {
            consumers[i] = new PriorityConsumer(topicName, i + 1, startSignal);
            executorService.submit(consumers[i]);
        }

        // 모든 컨슈머가 준비될 때까지 대기 후 시작 신호 전송
        try {
            Thread.sleep(3000);
            System.out.println("🔥 모든 컨슈머 시작!");
            startSignal.countDown(); // 모든 컨슈머 동시에 poll() 호출
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 종료 처리
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("🛑 컨슈머 종료 중...");
            for (PriorityConsumer consumer : consumers) {
                consumer.shutdown();
            }
            executorService.shutdown();
        }));
    }
}
