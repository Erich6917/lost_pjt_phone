package org.phone_consumer.kafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.phone_consumer.utils.PropertiesUtil;

public class HBaseConsumer {
    public static void main(String[] args) {
        //消费者API
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(PropertiesUtil.properties);
        //kafka Topic
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")));

        while (true) {
            //消费拉取数据
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            //遍历打印数据
            for (ConsumerRecord<String, String> cr:records) {
                String value = cr.value();
                System.out.println(value);
            }
        }	
    }
}
