package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.lang.Thread;
/**
 * This producer will send messages to topic "fast-messages" with the send timestamp in the payload
 * so that end-to-end latency can be calculated on the consumer. Send events are immediately flushed
 * in order to effectively disable send buffering.
 */
public class Producer {
    public static void main(String[] args) throws IOException {
        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }
        /**
         * Create a message of size msgSize in bytes
         */
        try {
            int[] lengths = {10,100,500,1000,2000,4000,6000,8000,9000,10000,12000,14000,16000,18000,20000,25000,30000,40000,50000};
            for (int length : lengths) {
                // Java chars are 2 bytes
                StringBuilder sb = new StringBuilder(length/2);
                sb.append('"');
                for (int i=0; i<length/2; i++) {
                    sb.append('a');
                }
                sb.append('"');

                for (int i = 1; i <= 10; i++) {
                    Thread.sleep(1000);
                    producer.send(new ProducerRecord<String, String>(
                            "fast-messages",
                            String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d, \"filler\":%s}", System.nanoTime() * 1e-9, i, sb)));
                    producer.flush();
                    Thread.sleep(1000);
                    //if (i % 1000 == 0) {
                    producer.send(new ProducerRecord<String, String>(
                            "fast-messages",
                            String.format("{\"type\":\"marker\", \"t\":%.3f, \"k\":%d, \"filler\":%s}", System.nanoTime() * 1e-9, i, sb)));
                    producer.flush();
                }
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }
    }
}