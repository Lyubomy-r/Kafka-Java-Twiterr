package KafkaPackJava.kafkaJava;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.util.Properties;


public class ProducerCallBack {

    public static void main(String[] args){

        final Logger logger= LoggerFactory.getLogger(ProducerCallBack.class);

        String boostrapServers = "127.0.0.1:9092";


        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );

        // creat the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        //send data
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                "first_topic"," message to kafka producer first_topic, callback "+ LocalTime.now());

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e==null){
                    logger.info("Received new metadata. \n"
                            +"Topic : "+recordMetadata.topic()+"\n"
                            +"Partition : "+ recordMetadata.partition()+"\n"
                            +"Offset : "+recordMetadata.offset()+ "\n"
                            +"Timestamp : "+ recordMetadata.timestamp()+"\n");
                }else{
                    logger.error("Error while producing",e);
                }
            }
        });

        producer.flush();
        producer.close();

    }

}
