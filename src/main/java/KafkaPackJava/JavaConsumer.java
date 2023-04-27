package KafkaPackJava;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class JavaConsumer {

    public static void main(String[] args) {

        final Logger logger= LoggerFactory.getLogger(JavaConsumer.class.getName());

        String boostrapServers = "127.0.0.1:9092";

        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//latest none

        KafkaConsumer<String,String>  consumer = new KafkaConsumer<String, String>(properties);

        // where consumer get message from one topic
//        consumer.subscribe(Collections.singleton("first_topic"));
        // where consumer get message from many  topics
//        consumer.subscribe(Arrays.asList("first_topic", "second_topic"));

        consumer.subscribe(Arrays.asList("first_topic"));

        while(true){

            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));

            for( ConsumerRecord<String,String> record:records ){
                logger.info("Key : "+record.key()+"; Value : "+record.value());
                logger.info("Partition : "+record.partition()+"; Offset : "+record.offset());
            }
        }
    }

}
