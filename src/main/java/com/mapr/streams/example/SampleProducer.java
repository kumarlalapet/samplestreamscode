package com.mapr.streams.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by mlalapet on 2/19/16.
 */
public class SampleProducer {
    // Set the stream and topic to publish to.
    public static String topic = "/demostreams/meetups:newmeetup";
    // Set the number of messages to send.
    public static int numMessages = 50;

    // Declare a new producer.
    public static KafkaProducer producer;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        configureProducer(args);
        for(int i = 0; i < numMessages; i++) {
            // Set content of each message.
            String messageText = "Msg " + i;

           /* Add each message to a record. A ProducerRecord object
              identifies the topic or specific partition to publish
	       a message to. */
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, messageText);

            // Send the record to the producer client library.
            producer.send(rec);
            System.out.println("Sent message number " + i);
        }
        producer.close();
        System.out.println("All done.");
    }
    /* Set the value for a configuration parameter.
       This configuration parameter specifies which class
       to use to serialize the value of each message.*/
    public static void configureProducer(String[] args) {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }
}
