package KafkaPackJava.kafkaJava;

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

public class JavaConsumerThreads {
    public  JavaConsumerThreads(){

    }

    public static void main(String[] args) {
        new JavaConsumerThreads().run();

    }

    public void run(){
        final Logger logger = LoggerFactory.getLogger(JavaConsumerThreads.class.getName());

        String boostrapServers = "127.0.0.1:9092";
        String topic = "first_topic";
        String groupId = "my-application";
        CountDownLatch latch=new  CountDownLatch(1);

        Runnable myConsumer = new ConsumerRunnable(boostrapServers,groupId,topic,latch);

        Thread myThread = new Thread(myConsumer);
        myThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumer).shutdown();
        }
        ));
        try {
            latch.await();
        }catch(InterruptedException e){
            e.printStackTrace();
            logger.error("Application got interrupted");
        }finally{
            logger.info("Application is closing");
        }
    }


    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String boostrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//latest none
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key : " + record.key() + "; Value : " + record.value());
                        logger.info("Partition : " + record.partition() + "; Offset : " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Receiver shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }

    }
}