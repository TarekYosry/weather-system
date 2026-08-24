--Battery Status Distribution
WITH BaseStats AS (
    SELECT 
        station_id,
        SUM(CASE WHEN battery_status = 'low' THEN 1 ELSE 0 END) AS low_cnt,
        SUM(CASE WHEN battery_status = 'medium' THEN 1 ELSE 0 END) AS med_cnt,
        SUM(CASE WHEN battery_status = 'high' THEN 1 ELSE 0 END) AS high_cnt,
        COUNT(*) AS total_msgs
    FROM weather_readings
    GROUP BY station_id
),
Percentages AS (
    SELECT 
        station_id::text AS station,
        ROUND((low_cnt * 100.0) / total_msgs, 2) AS low_pct,
        ROUND((med_cnt * 100.0) / total_msgs, 2) AS medium_pct,
        ROUND((high_cnt * 100.0) / total_msgs, 2) AS high_pct,
        0 AS sort_flag,
        station_id AS sort_val
    FROM BaseStats
)
SELECT station, low_pct, medium_pct, high_pct
FROM (
    SELECT station, low_pct, medium_pct, high_pct, sort_flag, sort_val FROM Percentages
    UNION ALL
    SELECT 
        'OVERALL AVG' AS station,
        ROUND(AVG(low_pct), 2) AS low_pct,
        ROUND(AVG(medium_pct), 2) AS medium_pct,
        ROUND(AVG(high_pct), 2) AS high_pct,
        1 AS sort_flag, 
        99999 AS sort_val
    FROM Percentages
) final_result
ORDER BY sort_flag, sort_val;


--Dropped Messages per Station
WITH StationDrops AS (
    SELECT 
        station_id::text AS station,
        MAX(sequence_number) AS expected_msgs,
        COUNT(sequence_number) AS received_msgs,
        (MAX(sequence_number) - COUNT(sequence_number)) AS dropped_msgs,
        ROUND(((MAX(sequence_number) - COUNT(sequence_number)) * 100.0) / MAX(sequence_number), 2) AS drop_rate_pct,
        0 AS sort_flag,
        station_id AS sort_val
    FROM weather_readings
    GROUP BY station_id
)
SELECT station, expected_msgs, received_msgs, dropped_msgs, drop_rate_pct
FROM (
    SELECT station, expected_msgs, received_msgs, dropped_msgs, drop_rate_pct, sort_flag, sort_val FROM StationDrops
    UNION ALL
    SELECT 
        'OVERALL AVG' AS station,
        ROUND(AVG(expected_msgs), 0) AS expected_msgs,
        ROUND(AVG(received_msgs), 0) AS received_msgs,
        ROUND(AVG(dropped_msgs), 0) AS dropped_msgs,
        ROUND(AVG(drop_rate_pct), 2) AS drop_rate_pct,
        1 AS sort_flag, 
        99999 AS sort_val
    FROM StationDrops
) final_result
ORDER BY sort_flag, sort_val;


--docker-compose exec -T postgres psql -U postgres -d weather_db < queries.sql