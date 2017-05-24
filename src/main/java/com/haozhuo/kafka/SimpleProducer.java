package com.haozhuo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 * 代码参看：https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm
 */
public class SimpleProducer {
    public static void main(String args[]) {
        String topicName = args.length > 0 ? args[0] : "test";
        Properties props = new Properties();
        props.put("client.id","112");
        props.put("bootstrap.servers","192.168.70.13:9092,192.168.70.12:9092,192.168.70.11:9092");
        props.put("acks","all");
        //Setting a value greater than zero will cause the producer to resend any record whose send fails with a potentially transient error
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 100);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for(int i = 0; i < 10; i++){
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)+"xxx"));
        }

        System.out.println("Message sent successfully");
        producer.close();
    }
}
