package com.anupam.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.time.LocalDateTime;
import java.util.Properties;

public class KafkaUtils {
    private static final String KAFKA_SERVERS = "kafka:9092";
    
    public static KafkaSource<String> createFlightEventSource() {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("auto.offset.reset", "latest");
        kafkaProps.setProperty("enable.auto.commit", "true");
        
        return KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SERVERS)
                .setTopics("flight-events")
                .setGroupId("flight-consumer-" + System.currentTimeMillis())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(kafkaProps)
                .build();
    }
    
    public static KafkaSink<String> createNotificationSink() {
        return KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flight-delay-notifications")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }
    
    public static String createDelayNotification(FlightEvent flight) {
        return String.format(
                "{\"flightId\":\"%s\",\"userId\":\"%s\",\"flightNumber\":\"%s\",\"airline\":\"%s\",\"route\":\"%s-%s\",\"delayMinutes\":%d,\"message\":\"Your flight %s is delayed by %d minutes\"}",
                flight.flightId, flight.userId, flight.flightNumber, flight.airline, 
                flight.origin, flight.destination, flight.delayMinutes, flight.flightNumber, flight.delayMinutes);
    }

    public static class FlightEvent {
        public String flightId;
        public String flightNumber;
        public String airline;
        public String origin;
        public String destination;
        public LocalDateTime scheduledTime;
        public LocalDateTime actualTime;
        public int isDelayed;
        public String userId;
        public long delayMinutes;
    }
}