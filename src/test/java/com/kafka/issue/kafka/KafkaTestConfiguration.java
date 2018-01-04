package com.kafka.issue.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

import com.google.common.collect.ImmutableMap;

public class KafkaTestConfiguration {

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Bean
    public KafkaTemplate<String, String> listenerTopicKafkaTemplate() {
        ProducerFactory<String, String> producerFactory = kafkaProducerFactoryForStringMessages();
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setMessageConverter(new StringJsonMessageConverter());
        kafkaTemplate.setDefaultTopic("listenerTopic");
        return kafkaTemplate;
    }

    @Bean
    public ProducerFactory<String, String> kafkaProducerFactoryForStringMessages() {
        Map<String, Object> props = ImmutableMap.<String, Object> builder()
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers).build();
        return new DefaultKafkaProducerFactory<>(props);
    }

}