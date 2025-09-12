Click House Table

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


-- Update flights table
ALTER TABLE flights.flights ADD COLUMN airline String AFTER flight_number;
ALTER TABLE flights.flights ADD COLUMN delay_minutes Int64 AFTER is_delayed;

-- Create aggregation tables
CREATE TABLE flights.airline_delay_stats (
    airline String,
    total_flights UInt64,
    delayed_flights UInt64,
    avg_delay_minutes Float64,
    delay_rate Float64,
    window_start DateTime,
    window_end DateTime
) ENGINE = MergeTree() ORDER BY (airline, window_start);

CREATE TABLE flights.route_delay_stats (
    route String,
    origin String,
    destination String,
    total_flights UInt64,
    avg_delay_minutes Float64,
    window_start DateTime,
    window_end DateTime
) ENGINE = MergeTree() ORDER BY (route, window_start);
 ENGINE = MergeTree() ORDER BY (route, window_start);

-- Hourly delay trends
CREATE TABLE flights.hourly_delay_stats (
    hour_of_day UInt8,
    total_flights UInt64,
    delayed_flights UInt64,
    avg_delay_minutes Float64,
    window_start DateTime,
    window_end DateTime
) ENGINE = MergeTree() ORDER BY (hour_of_day, window_start);

-- Update flights table to include airline and delay_minutes
ALTER TABLE flights.flights ADD COLUMN airline String AFTER flight_number;
ALTER TABLE flights.flights ADD COLUMN delay_minutes Int64 AFTER is_delayed;


Notification JSON:
{
  "flightId": "ABC123",
  "userId": "user456",
  "flightNumber": "AA100",
  "airline": "American",
  "route": "NYC-LAX",
  "delayMinutes": 45,
  "message": "Your flight AA100 is delayed by 45 minutes"
}

How to Check Log
docker logs -f taskmanager | Select-String -Pattern "(Checking flight|Publishing delay|Flight.*delayed)"

# Follow logs for your app only
docker-compose logs -f demo1-app

# Follow logs with timestamps
docker-compose logs -f -t demo1-app

# Last 50 lines then follow
docker-compose logs --tail=50 -f demo1-app

# External check (from your machine)
curl http://localhost:8081/scheduler/status

# Check health status
docker-compose ps








