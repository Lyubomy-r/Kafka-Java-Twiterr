package KafkaPackJava.kafkaJava;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class JavaProducer {

    public static void main(String[] args){

        String boostrapServers = "127.0.0.1:9092";

        // first way to create a Properties for Producer.
//          Properties properties = new Properties();
//          properties.setProperty("bootstrap.servers",boostrapServers );
//          properties.setProperty("key.serializer", StringSerializer.class.getName());
//          properties.setProperty("value.serializer", StringSerializer.class.getName() );

        // second way to create a Properties for Producer. we use
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers );
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );

        // creat the producer
            KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        //send data
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","second message to kafka producer first_topic");

        producer.send(record);

        producer.flush();
        producer.close();

    }
}
