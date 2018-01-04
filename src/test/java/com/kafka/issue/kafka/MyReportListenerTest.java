package com.kafka.issue.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.junit4.SpringRunner;

import com.kafka.Event;
import com.kafka.issue.kafka.KafkaTestConfiguration;
import com.kafka.issue.kafka.KafkaMessagesQueue;

@RunWith(SpringRunner.class)
@SpringBootTest
@Import(KafkaTestConfiguration.class)
public class MyReportListenerTest {

    private static final int EVENTS_POLL_TIMEOUT_SECONDS = 10;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Autowired
    @Qualifier("listenerTopicKafkaTemplate")
    private KafkaTemplate<String, String> kafkaTemplate;

    private KafkaMessagesQueue kafkaMessagesQueue;

    @Before
    public void setUp() throws Exception {
        kafkaMessagesQueue = new KafkaMessagesQueue(kafkaBootstrapServers);
    }

    @After
    public void stopListening() {
        kafkaMessagesQueue.stopListening();
    }

    @Test
    public void test() throws Exception {
        String payload = UUID.randomUUID().toString();
        Event event = new Event(payload);
        BlockingQueue<ConsumerRecord<String, String>> outgoingEvents = kafkaMessagesQueue.listenOnTopic("outputTopic");

        kafkaTemplate.send(new GenericMessage<>(event));

        ConsumerRecord<String, String> receivedEvent = outgoingEvents.poll(EVENTS_POLL_TIMEOUT_SECONDS,
                TimeUnit.SECONDS);
        Event receivedValue = new ObjectMapper().readValue(receivedEvent.value(), Event.class);
        assertThat(receivedValue.getPayload()).isEqualTo(payload);
    }
}