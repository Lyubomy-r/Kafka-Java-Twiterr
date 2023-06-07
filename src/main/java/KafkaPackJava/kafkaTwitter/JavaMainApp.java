package KafkaPackJava.kafkaTwitter;

public class JavaMainApp {

    public static void main(String[] args){
        TwitterProducer twitterProducer= new TwitterProducer();

        twitterProducer.run();

    }
}
