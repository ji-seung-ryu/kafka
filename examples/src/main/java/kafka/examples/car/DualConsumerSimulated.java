package kafka.examples.car;

import org.knowm.xchart.*;

import java.awt.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class DualConsumerSimulated {
    private static List<Long> sensorProcessingTimes = new CopyOnWriteArrayList<>();
    private static List<Long> logProcessingTimes = new CopyOnWriteArrayList<>();
    private static List<Integer> sensorIndexList = new CopyOnWriteArrayList<>();
    private static List<Integer> logIndexList = new CopyOnWriteArrayList<>();

    private static XYChart chart;
    private static SwingWrapper<XYChart> sw;

    public static void main(String[] args) {
        // 그래프 초기 설정
        setupChart();

        // 가짜 데이터 시뮬레이션 실행 (Kafka 없이)
        new Thread(() -> simulateMessageProcessing("Sensor", sensorProcessingTimes, sensorIndexList)).start();
        new Thread(() -> simulateMessageProcessing("Log", logProcessingTimes, logIndexList)).start();

        // 종료 시 통계 출력
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n📦 Simulated Consumers shutting down...");
            printProcessingTimeStats(sensorProcessingTimes, "Sensor");
            printProcessingTimeStats(logProcessingTimes, "Log");
        }));
    }

    private static void setupChart() {
        chart = new XYChart(800, 500);
        chart.setTitle("Kafka Processing Time");
        chart.setXAxisTitle("Message Count");
        chart.setYAxisTitle("Processing Time (ms)");
        chart.getStyler().setLegendVisible(true);
        chart.getStyler().setXAxisLabelRotation(45);
        chart.getStyler().setMarkerSize(5);
        chart.getStyler().setChartBackgroundColor(Color.WHITE);

        List<Integer> xInit = new ArrayList<>();
        List<Long> yInit = new ArrayList<>();
        xInit.add(0);
        yInit.add(0L);

        // 초기 시리즈 추가
        XYSeries sensorSeries = chart.addSeries("Sensor Processing Time", xInit, yInit);
        XYSeries logSeries = chart.addSeries("Log Processing Time", xInit, yInit);

        // 점만 찍히게 스타일 설정 (Scatter 모드)
        sensorSeries.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        logSeries.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);

        sw = new SwingWrapper<>(chart);
        sw.displayChart();
    }

    private static void simulateMessageProcessing(String type, List<Long> processingTimes, List<Integer> indexList) {
        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            long simulatedProcessingTime;

            if (type.equals("Sensor")) {
                // 5% 확률로 15ms, 아니면 0~10ms
                if (random.nextDouble() < 0.05) {
                    simulatedProcessingTime = 84;
                } else {
                    simulatedProcessingTime = random.nextInt(11); // 0~10ms
                }
            } else {

                    simulatedProcessingTime = 60 + random.nextInt(31); // 60~90ms

            }

            System.out.println("🎮 [" + type + " Sim] Processing: FakeMessage-" + i);
            System.out.println("🎮 [" + type + " Sim] Processing Time: " + simulatedProcessingTime + "ms\n");

            processingTimes.add(simulatedProcessingTime);
            indexList.add(i);

            if (processingTimes.size() > 100) {
                processingTimes.remove(0);
                indexList.remove(0);
            }

            updateGraph();

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void updateGraph() {
        try {
            List<Integer> sensorX = new ArrayList<>(sensorIndexList);
            List<Long> sensorY = new ArrayList<>(sensorProcessingTimes);
            List<Integer> logX = new ArrayList<>(logIndexList);
            List<Long> logY = new ArrayList<>(logProcessingTimes);

            chart.updateXYSeries("Sensor Processing Time", sensorX, sensorY, null);
            chart.updateXYSeries("Log Processing Time", logX, logY, null);
            sw.repaintChart();

        } catch (Exception e) {
            System.err.println("❗ Chart update failed: " + e.getMessage());
        }
    }

    private static void printProcessingTimeStats(List<Long> processingTimes, String type) {
        if (processingTimes.isEmpty()) {
            System.out.println("⚠️ [" + type + "] No processing time data available.");
            return;
        }

        List<Long> sortedTimes = new ArrayList<>(processingTimes);
        Collections.sort(sortedTimes);

        long min = sortedTimes.get(0);
        long max = sortedTimes.get(sortedTimes.size() - 1);
        double mean = sortedTimes.stream().mapToDouble(Long::doubleValue).average().orElse(0.0);
        long median = sortedTimes.get(sortedTimes.size() / 2);
        long q1 = sortedTimes.get(sortedTimes.size() / 4);
        long q3 = sortedTimes.get((sortedTimes.size() * 3) / 4);

        int count = sortedTimes.size();

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
