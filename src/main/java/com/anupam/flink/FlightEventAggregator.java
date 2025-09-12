package com.anupam.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
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

        KafkaSource<String> source = KafkaUtils.createFlightEventSource();
        
        LOG.info("Starting Flink job with Kafka source");

        ObjectMapper mapper = new ObjectMapper();

        DataStream<KafkaUtils.FlightEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(value -> {
                    LOG.info("Received Kafka message: {}", value);
                    JsonNode node = mapper.readTree(value);
                    KafkaUtils.FlightEvent e = new KafkaUtils.FlightEvent();
                    e.flightId = node.get("flightId").asText();
                    e.flightNumber = node.get("flightNumber").asText();
                    e.airline = node.get("airline").asText();
                    e.origin = node.get("origin").asText();
                    e.destination = node.get("destination").asText();
                    e.scheduledTime = LocalDateTime.parse(node.get("scheduledArrival").asText());
                    e.actualTime = LocalDateTime.parse(node.get("actualArrival").asText());
                    // Check both 'delayed' boolean field and 'status' string field
                    boolean delayed = false;
                    if (node.has("delayed")) {
                        delayed = node.get("delayed").asBoolean();
                    } else if (node.has("status")) {
                        String status = node.get("status").asText();
                        delayed = "DELAYED".equalsIgnoreCase(status);
                    }
                    e.isDelayed = delayed ? 1 : 0;
                    e.userId = node.get("userId").asText();
                    e.delayMinutes = Duration.between(e.scheduledTime, e.actualTime).toMinutes();
                    LOG.info("Parsed FlightEvent: flightId={}, delayed={}, delayMinutes={}, status={}", 
                            e.flightId, e.isDelayed, e.delayMinutes, node.has("status") ? node.get("status").asText() : "N/A");
                    
                    // Immediate notification check
                    if (e.isDelayed == 1) {
                        LOG.info("DELAYED FLIGHT DETECTED: flightId={}, delayMinutes={}", e.flightId, e.delayMinutes);
                    } else {
                        LOG.info("Flight {} is on time", e.flightId);
                    }
                    
                    return e;
                });

        // Send notifications for delayed flights
        events
                .filter(flight -> {
                    LOG.info("Filter check: flightId={}, isDelayed={}", flight.flightId, flight.isDelayed);
                    return flight.isDelayed == 1;
                })
                .map(flight -> {
                    String notification = KafkaUtils.createDelayNotification(flight);
                    LOG.info("SENDING NOTIFICATION TO KAFKA: {}", notification);
                    return notification;
                })
                .sinkTo(KafkaUtils.createNotificationSink());
                
        LOG.info("Notification stream configured");

        // 1. Airline delay rates (every 2 minutes for testing)
        events
                .keyBy(e -> e.airline)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .aggregate(new AirlineStatsAggregator())
                .addSink(JdbcSink.sink(
                        "INSERT INTO flights.airline_delay_stats (airline, total_flights, delayed_flights, avg_delay_minutes, delay_rate, window_start, window_end) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (PreparedStatement ps, AirlineStats stats) -> {
                            LOG.info("Inserting airline stats: airline={}, delayRate={}", stats.airline, stats.delayRate);
                            ps.setString(1, stats.airline);
                            ps.setLong(2, stats.totalFlights);
                            ps.setLong(3, stats.delayedFlights);
                            ps.setDouble(4, stats.avgDelayMinutes);
                            ps.setDouble(5, stats.delayRate);
                            ps.setTimestamp(6, java.sql.Timestamp.valueOf(LocalDateTime.now().minusMinutes(2)));
                            ps.setTimestamp(7, java.sql.Timestamp.valueOf(LocalDateTime.now()));
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(1000).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://clickhouse:8123/flights")
                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                .build()));

        // 2. Route delay stats (every 3 minutes for testing)
        events
                .keyBy(e -> e.origin + "-" + e.destination)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(3)))
                .aggregate(new RouteStatsAggregator())
                .addSink(JdbcSink.sink(
                        "INSERT INTO flights.route_delay_stats (route, origin, destination, total_flights, avg_delay_minutes, window_start, window_end) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (PreparedStatement ps, RouteStats stats) -> {
                            LOG.info("Inserting route stats: route={}, avgDelay={}", stats.route, stats.avgDelayMinutes);
                            ps.setString(1, stats.route);
                            ps.setString(2, stats.origin);
                            ps.setString(3, stats.destination);
                            ps.setLong(4, stats.totalFlights);
                            ps.setDouble(5, stats.avgDelayMinutes);
                            ps.setTimestamp(6, java.sql.Timestamp.valueOf(LocalDateTime.now().minusMinutes(3)));
                            ps.setTimestamp(7, java.sql.Timestamp.valueOf(LocalDateTime.now()));
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(1000).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://clickhouse:8123/flights")
                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                .build()));

        // 3. Hourly delay trends (every 5 minutes for testing)
        events
                .keyBy(e -> e.scheduledTime.getHour())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .aggregate(new HourlyStatsAggregator())
                .addSink(JdbcSink.sink(
                        "INSERT INTO flights.hourly_delay_stats (hour_of_day, total_flights, delayed_flights, avg_delay_minutes, window_start, window_end) VALUES (?, ?, ?, ?, ?, ?)",
                        (PreparedStatement ps, HourlyStats stats) -> {
                            LOG.info("Inserting hourly stats: hour={}, delayedFlights={}", stats.hourOfDay, stats.delayedFlights);
                            ps.setInt(1, stats.hourOfDay);
                            ps.setLong(2, stats.totalFlights);
                            ps.setLong(3, stats.delayedFlights);
                            ps.setDouble(4, stats.avgDelayMinutes);
                            ps.setTimestamp(5, java.sql.Timestamp.valueOf(LocalDateTime.now().minusMinutes(5)));
                            ps.setTimestamp(6, java.sql.Timestamp.valueOf(LocalDateTime.now()));
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(1000).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://clickhouse:8123/flights")
                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                .build()));

        // 4. Store raw flight data
        events
                .addSink(JdbcSink.sink(
                        "INSERT INTO flights.flights (flight_id, flight_number, airline, origin, destination, scheduled_time, actual_time, is_delayed, user_id, delay_minutes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (PreparedStatement ps, KafkaUtils.FlightEvent flight) -> {
                            LOG.info("Inserting flight: flightId={}, delayed={}", flight.flightId, flight.isDelayed);
                            ps.setString(1, flight.flightId);
                            ps.setString(2, flight.flightNumber);
                            ps.setString(3, flight.airline);
                            ps.setString(4, flight.origin);
                            ps.setString(5, flight.destination);
                            ps.setTimestamp(6, java.sql.Timestamp.valueOf(flight.scheduledTime));
                            ps.setTimestamp(7, java.sql.Timestamp.valueOf(flight.actualTime));
                            ps.setInt(8, flight.isDelayed);
                            ps.setString(9, flight.userId);
                            ps.setLong(10, flight.delayMinutes);
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(1000).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://clickhouse:8123/flights")
                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                .build()));

        env.execute("Flight Event Aggregator Job");
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

    public static class AirlineStats {
        public String airline;
        public long totalFlights;
        public long delayedFlights;
        public double avgDelayMinutes;
        public double delayRate;
    }

    public static class RouteStats {
        public String route;
        public String origin;
        public String destination;
        public long totalFlights;
        public double avgDelayMinutes;
    }

    public static class HourlyStats {
        public int hourOfDay;
        public long totalFlights;
        public long delayedFlights;
        public double avgDelayMinutes;
    }

    public static class AirlineStatsAggregator implements AggregateFunction<KafkaUtils.FlightEvent, AirlineStats, AirlineStats> {
        public AirlineStats createAccumulator() {
            return new AirlineStats();
        }
        
        public AirlineStats add(KafkaUtils.FlightEvent flight, AirlineStats acc) {
            acc.airline = flight.airline;
            acc.totalFlights++;
            if (flight.isDelayed == 1) acc.delayedFlights++;
            acc.avgDelayMinutes = (acc.avgDelayMinutes * (acc.totalFlights - 1) + flight.delayMinutes) / acc.totalFlights;
            acc.delayRate = acc.totalFlights > 0 ? (double) acc.delayedFlights / acc.totalFlights * 100 : 0;
            return acc;
        }
        
        public AirlineStats getResult(AirlineStats acc) { 
            return acc; 
        }
        
        public AirlineStats merge(AirlineStats a, AirlineStats b) {
            AirlineStats merged = new AirlineStats();
            merged.airline = a.airline;
            merged.totalFlights = a.totalFlights + b.totalFlights;
            merged.delayedFlights = a.delayedFlights + b.delayedFlights;
            if (merged.totalFlights > 0) {
                merged.avgDelayMinutes = (a.avgDelayMinutes * a.totalFlights + b.avgDelayMinutes * b.totalFlights) / merged.totalFlights;
                merged.delayRate = (double) merged.delayedFlights / merged.totalFlights * 100;
            }
            return merged;
        }
    }

    public static class RouteStatsAggregator implements AggregateFunction<KafkaUtils.FlightEvent, RouteStats, RouteStats> {
        public RouteStats createAccumulator() {
            return new RouteStats();
        }
        
        public RouteStats add(KafkaUtils.FlightEvent flight, RouteStats acc) {
            acc.route = flight.origin + "-" + flight.destination;
            acc.origin = flight.origin;
            acc.destination = flight.destination;
            acc.totalFlights++;
            acc.avgDelayMinutes = (acc.avgDelayMinutes * (acc.totalFlights - 1) + flight.delayMinutes) / acc.totalFlights;
            return acc;
        }
        
        public RouteStats getResult(RouteStats acc) { 
            return acc; 
        }
        
        public RouteStats merge(RouteStats a, RouteStats b) {
            RouteStats merged = new RouteStats();
            merged.route = a.route;
            merged.origin = a.origin;
            merged.destination = a.destination;
            merged.totalFlights = a.totalFlights + b.totalFlights;
            if (merged.totalFlights > 0) {
                merged.avgDelayMinutes = (a.avgDelayMinutes * a.totalFlights + b.avgDelayMinutes * b.totalFlights) / merged.totalFlights;
            }
            return merged;
        }
    }

    public static class HourlyStatsAggregator implements AggregateFunction<KafkaUtils.FlightEvent, HourlyStats, HourlyStats> {
        public HourlyStats createAccumulator() {
            return new HourlyStats();
        }
        
        public HourlyStats add(KafkaUtils.FlightEvent flight, HourlyStats acc) {
            acc.hourOfDay = flight.scheduledTime.getHour();
            acc.totalFlights++;
            if (flight.isDelayed == 1) acc.delayedFlights++;
            acc.avgDelayMinutes = (acc.avgDelayMinutes * (acc.totalFlights - 1) + flight.delayMinutes) / acc.totalFlights;
            return acc;
        }
        
        public HourlyStats getResult(HourlyStats acc) { 
            return acc; 
        }
        
        public HourlyStats merge(HourlyStats a, HourlyStats b) {
            HourlyStats merged = new HourlyStats();
            merged.hourOfDay = a.hourOfDay;
            merged.totalFlights = a.totalFlights + b.totalFlights;
            merged.delayedFlights = a.delayedFlights + b.delayedFlights;
            if (merged.totalFlights > 0) {
                merged.avgDelayMinutes = (a.avgDelayMinutes * a.totalFlights + b.avgDelayMinutes * b.totalFlights) / merged.totalFlights;
            }
            return merged;
        }
    }
}