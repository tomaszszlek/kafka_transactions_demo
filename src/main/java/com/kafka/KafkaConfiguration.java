package com.kafka;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;

import com.google.common.collect.ImmutableMap;

@Configuration
@EnableTransactionManagement
@EnableKafka
public class KafkaConfiguration {

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setMessageConverter(new StringJsonMessageConverter());
        return factory;
    }

    private <T> ConsumerFactory<String, T> consumerFactory() {
        Map<String, Object> properties = ImmutableMap.<String, Object> builder()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
                .put(ConsumerConfig.GROUP_ID_CONFIG, "events-consumers")
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class).build();
        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean
    public ProducerFactory<String, String> kafkaProducerFactory() {
        Map<String, Object> props = ImmutableMap.<String, Object> builder()
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers).build();
        DefaultKafkaProducerFactory<String, String> defaultKafkaProducerFactory = new DefaultKafkaProducerFactory<>(
                props);
        defaultKafkaProducerFactory.setTransactionIdPrefix("My-transaction-");
        return defaultKafkaProducerFactory;
    }

    @Bean
    public KafkaTemplate<String, String> outputKafkaTemplate() {
        ProducerFactory<String, String> producerFactory = kafkaProducerFactory();
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setMessageConverter(new StringJsonMessageConverter());
        kafkaTemplate.setDefaultTopic("outputTopic");
        return kafkaTemplate;
    }

    @Bean
    public KafkaTransactionManager kafkaTransactionManager() {
        KafkaTransactionManager manager = new KafkaTransactionManager(kafkaProducerFactory());
        manager.setFailEarlyOnGlobalRollbackOnly(true);
        manager.setNestedTransactionAllowed(true);
        manager.setValidateExistingTransaction(true);
        manager.setRollbackOnCommitFailure(true);
        manager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ALWAYS);
        return manager;
    }
}