package com.github.gkns1.kafka.elasticsearch.consumer;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ElasticSearchConsumer {

    private ElasticSearchConsumer() {

    }

    public void run(){
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-elasticsearch";
        String topic = "twitter_tweets";
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
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

            //create consumer
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

            RestHighLevelClient client = null;
            try {
                client = createClient();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    Integer recordCount = records.count();
                    logger.info("Received " + recordCount + " records.");

                    BulkRequest bulkRequest = new BulkRequest();
                    for (ConsumerRecord<String, String> record : records) {
                        //logger.info("Key: " + record.key() + ", Value " + record.value());
                        //logger.info("Partition: " + record.partition() + ", Offset " + record.offset());
                        try {
                            String id_tweet = extractIdFromTweet(record.value());
                            IndexRequest indexRequest = new IndexRequest("twitter");
                            indexRequest.source(record.value(), XContentType.JSON);
                            indexRequest.id(id_tweet);

                            bulkRequest.add(indexRequest);
                        } catch (NullPointerException e){
                            logger.warn("skipping bad data: " + record.value());
                        }
                        // IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                        // logger.info(indexResponse.getId());
                    }
                    if (recordCount > 0) {
                        BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                        consumer.commitSync();
                        logger.info("Offsets committed.");
                        Thread.sleep(1000);
                    }
                }
            }catch(WakeupException | IOException | InterruptedException e) {
                logger.info("Received shutdown signal!");
                try {
                    logger.info("Closing the client...");
                    client.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
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

       public static RestHighLevelClient createClient() throws IOException {

            InputStream input = new FileInputStream("kafka-consumer-elasticsearch/src/main/resources/config.properties");
            Properties prop = new Properties();
            prop.load(input);
           String hostname = prop.getProperty("hostname");
           String username = prop.getProperty("username");
           String password = prop.getProperty("password");

           final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

            RestClientBuilder builder = RestClient.builder(
                    new HttpHost(hostname, 443, "https"))
                    .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                            return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });
            RestHighLevelClient client = new RestHighLevelClient(builder);
            return client;

        }
        public static void main(String[] args) throws IOException {
            new ElasticSearchConsumer().run();

        }
        private static JsonParser jsonParser = new JsonParser();
        private static String extractIdFromTweet(String tweetJson){
           return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();

    }
}