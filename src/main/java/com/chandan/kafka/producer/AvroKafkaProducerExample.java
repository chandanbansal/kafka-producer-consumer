package com.chandan.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by chandan.bansal on 23/09/15.
 * this example will push the avro record in binary form to kafka.
 */
public class AvroKafkaProducerExample {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092,localhost:9093");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("request.required.acks", "1");


        ProducerConfig config = new ProducerConfig(props);
        Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

        Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/page_visits.avsc"));
        String site = "www.example.com";


        Random rnd = new Random();
        for (int nEvents = 0; nEvents < 5; nEvents++) {
            GenericRecord pageVisits = new GenericData.Record(schema);

            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);

            pageVisits.put("time", runtime);
            pageVisits.put("site", site);
            pageVisits.put("ip", ip);


            DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(schema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(pageVisits, encoder);
            encoder.flush();
            out.close();

            byte[] serializedBytes = out.toByteArray();

            KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>("page_visits_avro", serializedBytes);
            producer.send(data);
        }
        producer.close();

    }
}
