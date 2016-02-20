package com.mapr.streams.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Created by mlalapet on 2/19/16.
 */
public class SampleConsumer {
    // Set the stream and topic to read from.
    public static String topic = "/demostreams/meetups:newmeetup";

    // Declare a new consumer.
    public static KafkaConsumer consumer;

    public static void main(String[] args) throws IOException {
        configureConsumer(args);

        // Subscribe to the topic.
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        consumer.subscribe(topics);

        // Set the timeout interval for requests for unread messages.
        long pollTimeOut = 1000;

        boolean stop = false;
        int pollTimeout = 1000;
        while (!stop) {
            // Request unread messages from the topic.
            ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeout);
            Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
            if (iterator.hasNext()) {
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    // Iterate through returned records, extract the value
                    // of each message, and print the value to standard output.
                    System.out.println((" Consumed Record: " + record.toString()));
                }
            }
            else {
                System.out.println(" No message to consume ");
            }
        }
        consumer.close();
        System.out.println("All done.");
    }

    /* Set the value for a configuration parameter.
       This configuration parameter specifies which class
       to use to deserialize the value of each message.*/
    public static void configureConsumer(String[] args) {
        Properties props = new Properties();
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");


        consumer = new KafkaConsumer<String, String>(props);
    }
}

