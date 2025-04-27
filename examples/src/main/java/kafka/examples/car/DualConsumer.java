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
    private static List<Long> logProcessingTimes = new CopyOnWriteArrayList<>();
    private static List<Long> sensorTimestampList = new CopyOnWriteArrayList<>();
    private static List<Long> logTimestampList = new CopyOnWriteArrayList<>();


    private static XYChart chart;
    private static SwingWrapper<XYChart> sw;

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Properties props = new Properties();
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");         // 60초
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "15000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");     // 10분
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "600000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");


        // XChart 설정 (그래프)
        setupChart();

        // Kafka Consumer 실행 (각 Consumer를 별도 스레드에서 실행)
        executor.execute(() -> startConsumer(SENSOR_TOPIC, "sensor_group", sensorProcessingTimes, sensorTimestampList, "Sensor"));
        executor.execute(() -> startConsumer(LOG_TOPIC, "log_group", logProcessingTimes, logTimestampList, "Log"));

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

        // ✅ 여기 추가
        chart.getSeriesMap().get("Sensor Processing Time")
                .setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        chart.getSeriesMap().get("Log Processing Time")
                .setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);

        sw = new SwingWrapper<>(chart);
        sw.displayChart();
    }

    private static void startConsumer(String topic, String groupId, List<Long> processingTimes,
                                      List<Long> timestampList, String type) {
        Properties props = createConsumerProperties(groupId);
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("[Dual Consumer] " + type + " Consumer started...");

        long lastLogProcessTime = System.currentTimeMillis();
        final long logProcessInterval = 10_000; // 10초

        try {
            while (true) {
                long now = System.currentTimeMillis();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));


                if (!records.isEmpty()) {
                    processRecords(records, processingTimes, timestampList, now, type);
                    updateGraph();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
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

    private static void processRecords(ConsumerRecords<String, String> records, List<Long> processingTimes,
                                       List<Long> timestampList, long startTime, String type) {

        long processingTime = System.currentTimeMillis() - startTime;

        processingTimes.add(processingTime);
        timestampList.add(System.currentTimeMillis());

//        for (ConsumerRecord<String, String> record : records) {
//            long processingTime = System.currentTimeMillis() - startTime;
//            System.out.println("✅ [" + type + " Consumer] Processing: " + record.value());
//            System.out.println("✅ [" + type + " Consumer] Processing Time: " + processingTime + "ms\n");
//
//            processingTimes.add(processingTime);
//            timestampList.add(System.currentTimeMillis());
//
//            if (processingTimes.size() > 100) {
//                processingTimes.remove(0);
//                timestampList.remove(0);
//            }
//        }
    }

    private static void updateGraph() {
        List<Long> safeSensorTimestamps = new ArrayList<>(sensorTimestampList);  // ✅ 전체 복사
        List<Long> safeSensorProcessingTimes = new ArrayList<>(sensorProcessingTimes);
        List<Long> safeLogTimestamps = new ArrayList<>(logTimestampList);
        List<Long> safeLogProcessingTimes = new ArrayList<>(logProcessingTimes);

        int sensorSize = Math.min(safeSensorProcessingTimes.size(), safeSensorTimestamps.size());
        int logSize = Math.min(safeLogProcessingTimes.size(), safeLogTimestamps.size());

        if (sensorSize < 1 && logSize < 1) return;

        int sensorFrom = Math.max(0, sensorSize - 100);
        int logFrom = Math.max(0, logSize - 100);

        // ✅ 복사본에서 subList 뽑기
        List<Long> sensorX = safeSensorTimestamps.subList(sensorFrom, sensorSize);
        List<Long> sensorY = safeSensorProcessingTimes.subList(sensorFrom, sensorSize);
        List<Long> logX = safeLogTimestamps.subList(logFrom, logSize);
        List<Long> logY = safeLogProcessingTimes.subList(logFrom, logSize);

        chart.updateXYSeries("Sensor Processing Time", sensorX, sensorY, null);
        chart.updateXYSeries("Log Processing Time", logX, logY, null);
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


