CREATE TABLE flights.airline_delay_stats
(
    airline String,
    total_flights UInt64,
    delayed_flights UInt64,
    avg_delay_minutes Float64,
    window_start DateTime,
    window_end DateTime
)
ENGINE = MergeTree
ORDER BY (airline, window_start)
ClickHouse tables (DDL sketch)

CREATE TABLE users (user_id String, email String, phone String, notify_email UInt8, notify_sms UInt8, preferred_channel String, language String, opt_in UInt8, last_updated DateTime) ENGINE = MergeTree() ORDER BY user_id;

CREATE TABLE flights (flight_id String, flight_number String, origin String, destination String, scheduled_time DateTime, actual_time DateTime, is_delayed UInt8, user_id String) ENGINE = MergeTree() ORDER BY flight_id;

CREATE TABLE notifications (notification_id UUID, user_id String, flight_id String, channel String, contact String, status String, created_at DateTime, delivered_at DateTime, last_error String) ENGINE = MergeTree() ORDER BY notification_id;

CREATE TABLE notification_deliveries (notification_id UUID, provider String, response_code Int, response_message String, retried_count Int, last_attempt_at DateTime) ENGINE = MergeTree() ORDER BY notification_id;

    SHOW DATABASES;
    USE flights;
    SHOW TABLES;
    SHOW TABLES FROM flights;

docker cp build\libs\flight-flink-job.jar jobmanager:/opt/flink/
docker exec -it jobmanager ./bin/flink run /opt/flink/flight-flink-job.jar

docker-compose up -d
docker-compose stop