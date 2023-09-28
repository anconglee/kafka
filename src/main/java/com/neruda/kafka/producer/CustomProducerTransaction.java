package com.neruda.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * @Description:
 * @Author anconglee
 * @Date 2023/9/21 17:23
 */

public class CustomProducerTransaction {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.56.101:9092,192.168.56.102:9092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        producer.initTransactions();
        producer.beginTransaction();
        try {
            for (int i = 0 ; i<5 ; i++) {
                producer.send(new ProducerRecord<>("first","neruda"+i));
            }
            producer.commitTransaction();
            int i = 3/0;
        }catch (Exception e){
            producer.abortTransaction();
            System.out.println(e.getCause());
        }finally {
            producer.close();
        }
    }
}
