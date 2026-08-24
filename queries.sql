--Battery Status Distribution
SELECT 
    station_id,
    battery_status,
    COUNT(*) AS status_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY station_id), 2) AS percentage_dist
FROM 
    weather_readings
GROUP BY 
    station_id, 
    battery_status
ORDER BY 
    station_id, 
    battery_status;


--Dropped Messages per Station
SELECT 
    station_id,
    MAX(sequence_number) AS expected_messages,
    COUNT(sequence_number) AS received_messages,
    (MAX(sequence_number) - COUNT(sequence_number)) AS dropped_messages,
    ROUND(((MAX(sequence_number) - COUNT(sequence_number)) * 100.0) / MAX(sequence_number), 2) AS drop_rate_percentage
FROM 
    weather_readings
GROUP BY 
    station_id
ORDER BY 
    station_id;


--docker-compose exec -T postgres psql -U postgres -d weather_db < queries.sql