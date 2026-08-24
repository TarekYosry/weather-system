package com.netcentric.lab4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CentralStation {

    private static final String DB_URL = "jdbc:postgresql://127.0.0.1:5432/weather_db";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "password";

    public static void main(String[] args) {
        
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("weather_data"));

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            System.out.println("Connected to PostgreSQL database.");
            
            String createTableSQL = "CREATE TABLE IF NOT EXISTS weather_records (" +
                    "id SERIAL PRIMARY KEY, " +
                    "station_id BIGINT, " +
                    "s_no BIGINT, " +
                    "battery_status VARCHAR(50), " +
                    "status_timestamp BIGINT, " +
                    "humidity INT, " +
                    "temperature INT, " +
                    "wind_speed INT)";
            try (PreparedStatement createStmt = conn.prepareStatement(createTableSQL)) {
                createStmt.execute();
            }

            String insertSQL = "INSERT INTO weather_records (station_id, s_no, battery_status, status_timestamp, humidity, temperature, wind_speed) VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement insertStmt = conn.prepareStatement(insertSQL);

            System.out.println("CentralStation started. Listening to weather_data...");
            
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String jsonString = record.value();
                    JsonObject jsonObject = JsonParser.parseString(jsonString).getAsJsonObject();
                    JsonObject weather = jsonObject.getAsJsonObject("weather");

                    insertStmt.setLong(1, jsonObject.get("station_id").getAsLong());
                    insertStmt.setLong(2, jsonObject.get("s_no").getAsLong());
                    insertStmt.setString(3, jsonObject.get("battery_status").getAsString());
                    insertStmt.setLong(4, jsonObject.get("status_timestamp").getAsLong());
                    insertStmt.setInt(5, weather.get("humidity").getAsInt());
                    insertStmt.setInt(6, weather.get("temperature").getAsInt());
                    insertStmt.setInt(7, weather.get("wind_speed").getAsInt());

                    insertStmt.executeUpdate();
                    System.out.println("Saved to DB: Sequence No " + jsonObject.get("s_no").getAsLong());
                }
            }
        } catch (Exception e) {
            System.err.println("Database connection failed or interrupted.");
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}