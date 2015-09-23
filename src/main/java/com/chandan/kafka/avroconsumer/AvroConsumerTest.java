package com.chandan.kafka.avroconsumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;

/**
 * Created by chandan.bansal on 23/09/15.
 */
public class AvroConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;

    public AvroConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
            byte[] received_message = it.next().message();
            System.out.println("received bytes-" + received_message);
            Schema schema = null;

            try {
                schema = new Schema.Parser().parse(new File("src/main/resources/avro/page_visits.avsc"));
                DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(received_message, null);
                GenericRecord payload2 = null;
                payload2 = reader.read(null, decoder);
                System.out.println("Thread " + m_threadNumber + ": " + payload2);
            } catch(Exception IOException) {

            }
        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
