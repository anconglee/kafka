package com.neruda.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Description:
 * @Author anconglee
 * @Date 2023/9/21 17:23
 */
public class CustomProducerCallback {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.56.101:9092,192.168.56.102:9092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0 ; i<5 ; i++) {
            producer.send(new ProducerRecord<>("first", "neruda" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        System.out.println("主题："+recordMetadata.topic() + " 分区："+recordMetadata.partition());
                    }
                }
            });
            //Thread.sleep(1000);
        }

        producer.close();

    }
}
