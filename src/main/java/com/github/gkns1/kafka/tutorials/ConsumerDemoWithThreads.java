package com.github.gkns1.kafka.tutorials;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads() {

    }

    public void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "consumer_app_threads";
        String topic = "first_topic";
        //latch for threads
        CountDownLatch latch = new CountDownLatch(1);
        //consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                latch,
                topic,
                bootstrapServers,
                groupId
        );
        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {

            private CountDownLatch latch;
            private KafkaConsumer<String, String> consumer;
            private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
            public ConsumerRunnable(CountDownLatch latch,
                                    String topic,
                                    String bootstrapServers,
                                    String groupId) {
                this.latch = latch;
                //create consumer configs
                Properties properties = new Properties();
                properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                //create consumer
                consumer = new KafkaConsumer<>(properties);
                consumer.subscribe(Arrays.asList(topic));
            }

            @Override
            public void run() {
                try {
                    while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                        for (ConsumerRecord<String, String> record : records) {
                            logger.info("Key: " + record.key() + ", Value " + record.value());
                            logger.info("Partition: " + record.partition() + ", Offset " + record.offset());
                        }
                    }
                }catch(WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
                }
            }
            public void shutdown(){
                //will throw WakeUpException
                    consumer.wakeup();
            }
    }
}