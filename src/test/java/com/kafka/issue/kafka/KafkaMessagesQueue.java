package com.kafka.issue.kafka;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.utils.ContainerTestUtils;

import com.google.common.collect.ImmutableMap;

public class KafkaMessagesQueue {

    private static final int TOPIC_PARTITIONS_NUMBER = 1;

    private static final String CONSUMER_GROUP_PREFIX = "test-consumer-";

    private final String kafkaBootstrapServers;
    
    private KafkaMessageListenerContainer<String, String> listenerContainer;

    public KafkaMessagesQueue(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public BlockingQueue<ConsumerRecord<String, String>> listenOnTopic(String topic) throws Exception {
        BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

        listenerContainer = createKafkaListenerContainer(kafkaBootstrapServers, topic);
        listenerContainer.setupMessageListener((MessageListener<String, String>) records::add);
        listenerContainer.start();
        ContainerTestUtils.waitForAssignment(listenerContainer, TOPIC_PARTITIONS_NUMBER);
        return records;
    }

    public void stopListening() {
        if (listenerContainer != null) {
            listenerContainer.stop();
        }
    }

    private KafkaMessageListenerContainer<String, String> createKafkaListenerContainer(String kafkaBootstrapServers,
            String topic) {
        ConsumerFactory<String, String> consumerFactory = createConsumerFactory(kafkaBootstrapServers);

        return new KafkaMessageListenerContainer<>(consumerFactory, new ContainerProperties(topic));
    }

    private ConsumerFactory<String, String> createConsumerFactory(String kafkaBootstrapServers) {
        Map<String, Object> consumerProps = ImmutableMap.<String, Object> builder()
                .put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
                .put(GROUP_ID_CONFIG, CONSUMER_GROUP_PREFIX + System.currentTimeMillis())
                .put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class).build();
        return new DefaultKafkaConsumerFactory<>(consumerProps);
    }
}
