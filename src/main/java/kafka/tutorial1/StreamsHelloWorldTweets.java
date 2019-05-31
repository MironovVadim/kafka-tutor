package kafka.tutorial1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsHelloWorldTweets {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo");
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Object, String> firstTopicStream = builder.stream("first_topic");
        KStream<Object, String> filteredStream = firstTopicStream.filter((k, v) -> {
                    return v.endsWith("1");
//            v.endsWith("1")});
                });
        filteredStream.to("filtered_topic");

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);

        kafkaStreams.start();
    }
}
