package com.chandan.kafka.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * Created by chandan.bansal on 23/09/15.
 */
public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
            byte[] received_message = it.next().message();
            System.out.println("Thread " + m_threadNumber + ": " + new String(received_message));
        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
