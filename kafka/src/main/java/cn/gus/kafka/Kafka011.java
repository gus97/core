package cn.gus.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.Collections;
import java.util.Properties;

public class Kafka011 {

    public static void consumer() throws InterruptedException {

        Properties props = new Properties();

        props.put("bootstrap.servers", "172.18.111.7:9092,172.18.111.8:9092,172.18.111.7:9092");

        props.put("group.id", "gus001");

        props.put("enable.auto.commit", "true");

        props.put("auto.commit.interval.ms", "1000");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("test01"));

        boolean flag = true;

        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                Thread.sleep(100L);
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
        consumer.close();
    }

    public static void producer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.18.111.7:9092,172.18.111.8:9092,172.18.111.9:9092");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            System.out.println("top1>>> send " + i);
            producer.send(new ProducerRecord<>("test01", "test01" + i, "test01" + i));
        }
        producer.close();
    }
}
