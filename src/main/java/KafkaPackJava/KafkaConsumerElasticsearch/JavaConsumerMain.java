package KafkaPackJava.KafkaConsumerElasticsearch;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.Duration;

import static KafkaPackJava.KafkaConsumerElasticsearch.ElasticSearchConsumer.createClient;


public class JavaConsumerMain {
    public static void main(String[] args) throws IOException {
        KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig();

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = kafkaConsumerConfig.createConsumer("first_topic");
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                String jsonString  = record.value();

                IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
                        .source(jsonString, XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                logger.info(id);

            }

            client.close();
        }
    }
}
