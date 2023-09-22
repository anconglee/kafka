package com.neruda.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Description: 自定义分区器
 * @Author anconglee
 * @Date 2023/9/22 15:17
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        int partition;
        if (o1.toString().contains("neruda")){
            partition=1;
        }else {
            partition = 0;
        }
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
