# Kafka Priority Request Queue for Edge Computing

This project implements a priority-based message handling mechanism on top of **Apache Kafka** to improve real-time performance in edge computing environments. It customizes Kafka’s internal request queue logic to process messages based on their priority, enabling high-priority data (e.g., sensor data) to be consumed faster than low-priority data (e.g., log data).

## 🔍 Overview

Apache Kafka is a distributed messaging system designed for high throughput and reliability. However, it does not natively support message prioritization. In real-world applications such as autonomous vehicles or industrial IoT, data streams often have different urgency levels. This project modifies Kafka's core to support **priority-aware processing** while preserving performance and stability.

## 🎯 Goal

- Analyze Kafka’s internal architecture, focusing on the request dispatch pipeline.
- Replace or extend the existing `ArrayBlockingQueue` with a `PriorityBlockingQueue`.
- Ensure messages are processed in order of importance using `max_wait_time_ms` thresholds.
- Evaluate the system using multiple producers and consumers under simulated edge scenarios.

## 🛠️ Approaches

Three strategies were considered:

1. **PriorityBlockingQueue Replacement**  
   Replace Kafka's internal request queue with a `PriorityBlockingQueue` that ranks requests by importance.

2. **Dual-Queue Architecture**  
   Separate high-priority and low-priority requests into different queues to minimize performance loss.

3. **Reinsertion Strategy (Implemented)**  
   Retain the original queue but delay low-priority processing by reinserting requests until their `max_wait_time_ms` expires.

We implemented the third strategy, which avoids structural changes to Kafka and ensures **O(1) throughput** with effective prioritization.

## 🧪 Experiment Scenario

We simulated an **autonomous driving system** with two Kafka topics:

- `topic-sensor`: High-priority real-time sensor data (20 Hz)
- `topic-log`: Low-priority vehicle log data (1 Hz)

Consumers were configured to prioritize `sensor` messages based on custom-defined waiting thresholds.

### Configuration

| Parameter            | Sensor Data        | Log Data         |
|----------------------|--------------------|------------------|
| Frequency            | 20Hz               | 1Hz              |
| Record Size          | 132 bytes          | 130 bytes        |
| Topic                | `topic-sensor`     | `topic-log`      |

### Results Summary

| Scenario                                | Sensor Avg (ms) | Log Avg (ms) |
|-----------------------------------------|------------------|---------------|
| Default Kafka Queue, 1P-1C              | 73.45            | 465.92        |
| Default Kafka Queue, 3P-3C              | 183.69           | 202.88        |
| Custom Priority Queue, 1P-1C            | 76.32            | 1012.12       |
| Custom Priority Queue, 3P-3C            | 38.49            | 566.62        |

Sensor messages were always processed faster, confirming that priority logic works even under contention.

You can run the experiment by launching ZooKeeper and Kafka server, then executing the producers and consumer classes.

## ▶️ How to Run

1. Clone this repository and checkout the `3.5-threshold` branch.
2. Start Kafka and ZooKeeper services.
3. Run `SensorProducer.java` and `LogProducer.java` to publish data.
4. Run `DualConsumer.java` to observe processing latency with graphs.

GitHub Repository: [🔗 View Source Code](https://github.com/ji-seung-ryu/kafka/tree/3.5-threshold/examples/src/main/java/kafka/examples/car)

