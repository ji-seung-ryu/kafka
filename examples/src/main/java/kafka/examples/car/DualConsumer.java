package kafka.examples.car;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.knowm.xchart.*;

import java.awt.*;
import java.time.Duration;
import java.util.*;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DualConsumer {
    private static final String SENSOR_TOPIC = "topic_sensor";
    private static final String LOG_TOPIC = "topic_log";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static List<Long> sensorProcessingTimes = new CopyOnWriteArrayList<>();
    private static List<Long> logProcessingTimes =new CopyOnWriteArrayList<>();
    private static List<Long> timestampList =new CopyOnWriteArrayList<>();

    private static XYChart chart;
    private static SwingWrapper<XYChart> sw;

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // XChart 설정 (그래프)
        setupChart();

        // Kafka Consumer 실행 (각 Consumer를 별도 스레드에서 실행)
        executor.execute(() -> startConsumer(SENSOR_TOPIC, "sensor_group", sensorProcessingTimes, "Sensor"));
        executor.execute(() -> startConsumer(LOG_TOPIC, "log_group", logProcessingTimes, "Log"));

        // 안전한 종료 처리
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            System.out.println("Kafka Consumers shutting down...");
            printProcessingTimeStats(sensorProcessingTimes, "Sensor");
            printProcessingTimeStats(logProcessingTimes, "Log");
        }));
    }

    private static void setupChart() {
        chart = new XYChart(800, 500);
        chart.setTitle("Kafka Processing Time Comparison");
        chart.setXAxisTitle("Timestamp (ms)");
        chart.setYAxisTitle("Processing Time (ms)");
        chart.getStyler().setLegendVisible(true);
        chart.getStyler().setXAxisLabelRotation(45);
        chart.getStyler().setMarkerSize(5);
        chart.getStyler().setChartBackgroundColor(Color.WHITE);

        // 초기 데이터 추가
        List<Integer> xInit = new ArrayList<>();
        List<Long> yInit = new ArrayList<>();
        xInit.add(0);
        yInit.add(0L);

        chart.addSeries("Sensor Processing Time", xInit, yInit);
        chart.addSeries("Log Processing Time", xInit, yInit);

        sw = new SwingWrapper<>(chart);
        sw.displayChart();
    }

    private static void startConsumer(String topic, String groupId, List<Long> processingTimes, String type) {
        Properties props = createConsumerProperties(groupId);
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("[Dual Consumer] " + type + " Consumer started...");

        try {
            while (true) {
                long startTime = System.currentTimeMillis();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                processRecords(records, processingTimes, startTime, type);
                updateGraph();

                if (groupId.equals("sensor_group")) {
                    Thread.sleep(50); // 50ms 간격으로 데이터 컨슘 (실시간)
                } else {
                    Thread.sleep(10000); // 10초 간격으로 로그 데이터 컨슘
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            consumer.close();
            System.out.println("[Dual Consumer] " + type + " Consumer stopped.");
        }
    }

    private static Properties createConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (groupId.equals("sensor_group")) {
            props.put("priority", 1);
        }
        if (groupId.equals("log_group")) {
            props.put("priority", 5);
        }
        return props;
    }

    private static void processRecords(ConsumerRecords<String, String> records, List<Long> processingTimes, long startTime, String type) {
        for (ConsumerRecord<String, String> record : records) {
            long processingTime = System.currentTimeMillis() - startTime;
            System.out.println("✅ [" + type + " Consumer] Processing: " + record.value());
            System.out.println("✅ [" + type + " Consumer] Processing Time: " + processingTime + "ms\n");

            processingTimes.add(processingTime);
            timestampList.add(System.currentTimeMillis()); // 🚀 X축 데이터 추가

            if (processingTimes.size() > 100) {
                processingTimes.remove(0);
                timestampList.remove(0); // 🛠 X축 데이터도 함께 정리
            }
        }
    }

    private static void updateGraph() {
        if (timestampList.isEmpty()) {
            return; // 🚨 X축 데이터가 없으면 실행 안 함
        }

        int minSize = Math.min(timestampList.size(), Math.min(sensorProcessingTimes.size(), logProcessingTimes.size()));

        if (minSize < 1) {
            return; // 🚨 최소한 하나 이상의 데이터가 있어야 실행
        }

        int fromIndex = Math.max(0, minSize - 1000); // 🚀 100보다 작은 경우도 처리 가능하도록 변경

        List<Long> xData = timestampList.subList(fromIndex, minSize);
        List<Long> sensorData = sensorProcessingTimes.subList(fromIndex, minSize);
        List<Long> logData = logProcessingTimes.subList(fromIndex, minSize);

        chart.updateXYSeries("Sensor Processing Time", xData, sensorData, null);
        chart.updateXYSeries("Log Processing Time", xData, logData, null);
        sw.repaintChart();
    }

    private static void printProcessingTimeStats(List<Long> processingTimes, String type) {
        if (processingTimes.isEmpty()) {
            System.out.println("⚠️ [" + type + "] No processing time data available.");
            return;
        }

        // 정렬된 리스트 생성
        List<Long> sortedTimes = new ArrayList<>(processingTimes);
        Collections.sort(sortedTimes);

        // 통계 계산
        long min = sortedTimes.get(0);
        long max = sortedTimes.get(sortedTimes.size() - 1);
        double mean = sortedTimes.stream().mapToDouble(Long::doubleValue).average().orElse(0.0);
        long median = sortedTimes.get(sortedTimes.size() / 2);
        long q1 = sortedTimes.get(sortedTimes.size() / 4);
        long q3 = sortedTimes.get((sortedTimes.size() * 3) / 4);

        int count = sortedTimes.size();

        // 결과 출력
        System.out.println("📊 [" + type + "] Processing Time Statistics:");
        System.out.println("   - Total Messages: " + count);
        System.out.println("   - Min: " + min + " ms");
        System.out.println("   - Max: " + max + " ms");
        System.out.println("   - Mean: " + String.format("%.2f", mean) + " ms");
        System.out.println("   - Median: " + median + " ms");
        System.out.println("   - Q1 (25%): " + q1 + " ms");
        System.out.println("   - Q3 (75%): " + q3 + " ms");
        System.out.println();
    }



}


