package com.anupam.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Properties;

public class FlightEventAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(FlightEventAggregator.class);
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("auto.offset.reset", "latest");
        kafkaProps.setProperty("enable.auto.commit", "true");
        
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092,localhost:9092")
                .setTopics("flight-events")
                .setGroupId("flight-consumer-" + System.currentTimeMillis())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(kafkaProps)
                .build();
        
        LOG.info("Starting Flink job with Kafka source");

        ObjectMapper mapper = new ObjectMapper();

        DataStream<FlightEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(value -> {
                    LOG.info("Received Kafka message: {}", value);
                    JsonNode node = mapper.readTree(value);
                    FlightEvent e = new FlightEvent();
                    e.airline = node.get("airline").asText();
                    e.status = node.get("status").asText();
                    try {
                        LocalDateTime scheduled = LocalDateTime.parse(node.get("scheduledArrival").asText());
                        LocalDateTime actual = LocalDateTime.parse(node.get("actualArrival").asText());
                        e.delayMinutes = Duration.between(scheduled, actual).toMinutes();
                    } catch (Exception ex) {
                        e.delayMinutes = 0;
                    }
                    LOG.info("Parsed FlightEvent: airline={}, status={}, delay={}", e.airline, e.status, e.delayMinutes);
                    return e;
                });

        events
                .keyBy(e -> {
                    LOG.info("Keying event by airline: {}", e.airline);
                    return e.airline;
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .aggregate(new DelayAggregator())
                .map(agg -> {
                    LOG.info("Aggregated result: airline={}, total={}, delayed={}, avgDelay={}", agg.airline, agg.total, agg.delayed, agg.avgDelay);
                    return agg;
                })
                .addSink(JdbcSink.sink(
                        "INSERT INTO flights.airline_delay_stats " +
                                "(airline, total_flights, delayed_flights, avg_delay_minutes, window_start, window_end) " +
                                "VALUES (?, ?, ?, ?, now(), now())",
                        (PreparedStatement ps, AirlineAggregate agg) -> {
                            LOG.info("Inserting into ClickHouse: airline={}, total={}, delayed={}, avgDelay={}", 
                                    agg.airline, agg.total, agg.delayed, agg.avgDelay);
                            ps.setString(1, agg.airline);
                            ps.setLong(2, agg.total);
                            ps.setLong(3, agg.delayed);
                            ps.setDouble(4, agg.avgDelay);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1)
                                .withBatchIntervalMs(1000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://clickhouse:8123/flights")
                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                .build()
                ));

        env.execute("Flight Event Aggregator Job");
    }

    public static class FlightEvent {
        public String airline;
        public String status;
        public long delayMinutes;
    }

    public static class AirlineAggregate {
        public String airline;
        public long total;
        public long delayed;
        public double avgDelay;
    }

    public static class DelayAggregator implements AggregateFunction<FlightEvent, AirlineAggregate, AirlineAggregate> {
        private static final Logger AGG_LOG = LoggerFactory.getLogger(DelayAggregator.class);
        
        public AirlineAggregate createAccumulator() {
            AGG_LOG.info("Creating new accumulator");
            AirlineAggregate agg = new AirlineAggregate();
            agg.total = 0; agg.delayed = 0; agg.avgDelay = 0;
            return agg;
        }
        @Override
        public AirlineAggregate add(FlightEvent value, AirlineAggregate acc) {
            AGG_LOG.info("Adding event to accumulator: airline={}, status={}", value.airline, value.status);
            acc.airline = value.airline != null ? value.airline : acc.airline;

            acc.total += 1;
            if ("DELAYED".equals(value.status)) {
                acc.delayed += 1;
            }

            // Running average of delay minutes
            acc.avgDelay = ((acc.avgDelay * (acc.total - 1)) + value.delayMinutes) / acc.total;
            return acc;
        }

        @Override
        public AirlineAggregate getResult(AirlineAggregate acc) {
            return acc;
        }

        @Override
        public AirlineAggregate merge(AirlineAggregate a, AirlineAggregate b) {
            AirlineAggregate merged = new AirlineAggregate();
            merged.airline = (a.airline != null && !a.airline.isEmpty()) ? a.airline : b.airline;

            merged.total = a.total + b.total;
            merged.delayed = a.delayed + b.delayed;

            // Weighted average for the combined aggregates
            if (merged.total > 0) {
                merged.avgDelay = (a.avgDelay * a.total + b.avgDelay * b.total) / merged.total;
            } else {
                merged.avgDelay = 0.0;
            }

            return merged;
        }
    }
}
