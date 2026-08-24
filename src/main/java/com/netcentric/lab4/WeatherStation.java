package com.netcentric.lab4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.google.gson.JsonObject;

import java.util.Properties;
import java.util.Random;

public class WeatherStation {

    
    public static void main(String[] args) {
        
        Properties properties = new Properties();
        String kafkaBroker = System.getenv("KAFKA_BROKER") != null ? System.getenv("KAFKA_BROKER") : "kafka:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);        
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Random random = new Random();
        
        long stationId;
        try {
            String ip = java.net.InetAddress.getLocalHost().getHostAddress();
            stationId = Long.parseLong(ip.substring(ip.lastIndexOf('.') + 1));
        } catch (Exception e) {
            stationId = (long) (Math.random() * 10000) + 1;
        }
        long sequenceNumber = 1L;
        String topicName = "weather_data";

        try {
            while (true) {
                Thread.sleep(1000); 

                long currentSeq = sequenceNumber++;
                if (random.nextInt(100) < 10) {
                    System.out.println("Message dropped.");
                    continue; 
                }

                int batteryRand = random.nextInt(100);
                String batteryStatus;
                if (batteryRand < 30) {
                    batteryStatus = "low";
                } else if (batteryRand < 70) {
                    batteryStatus = "medium";
                } else {
                    batteryStatus = "high";
                }

                JsonObject weather = new JsonObject();
                weather.addProperty("humidity", random.nextInt(101)); 
                weather.addProperty("temperature", random.nextInt(120)); 
                weather.addProperty("wind_speed", random.nextInt(150)); 

                JsonObject message = new JsonObject();
                message.addProperty("station_id", stationId);
                message.addProperty("sequence_number", currentSeq);
                message.addProperty("battery_status", batteryStatus);
                message.addProperty("timestamp", System.currentTimeMillis() / 1000L);
                message.add("weather", weather);

                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message.toString());
                producer.send(record);

                System.out.println("Sent: " + message.toString());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}