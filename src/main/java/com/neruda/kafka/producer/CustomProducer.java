package com.neruda.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Description:
 * @Author anconglee
 * @Date 2023/9/21 17:23
 */
public class CustomProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.56.101:9092,192.168.56.102:9092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0 ; i<5 ; i++) {
            producer.send(new ProducerRecord<>("first","neruda"+i));
        }

        producer.close();

    }
}
