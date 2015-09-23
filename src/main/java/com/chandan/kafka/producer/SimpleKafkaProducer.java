package com.chandan.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by chandan.bansal on 07/08/15.
 */
public class SimpleKafkaProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092,localhost:9093");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        Random rnd = new Random();
        for (int nEvents = 0; nEvents < 10; nEvents++) {
            String runtime = new Date().toString();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ", www.example.com, " + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits_string", ip, msg);
            producer.send(data);
        }
        producer.close();
    }
}
