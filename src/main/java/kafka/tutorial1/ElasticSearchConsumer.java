package kafka.tutorial1;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class ElasticSearchConsumer {

    public static RestHighLevelClient elasticSearchConsumer() {
        String hostname = "el-search-4723054832.ap-southeast-2.bonsaisearch.net";
        String login = "o8ke39lvf";
        String password = "jqtu46c588";


        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(login, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback((httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)));
        return new RestHighLevelClient(builder);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

        KafkaConsumer<String, String> consumer = ConsumerDemoGroups.kafkaConsumer();
        RestHighLevelClient client = ElasticSearchConsumer.elasticSearchConsumer();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord cr : records) {
                Object value = cr.value();
                IndexRequest request = new IndexRequest("twitter", "tweets")
                        .source(value, XContentType.JSON);

                IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
                logger.info(indexResponse.getId());
                Thread.sleep(1000);
            }
        }
//        client.close();
    }
}
