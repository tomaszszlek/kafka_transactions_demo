package com.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Service;

@Service
public class MyService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyService.class);

    private final KafkaTemplate<String, String> outputKafkaTemplate;

    @Autowired
    public MyService(@Qualifier("outputKafkaTemplate") KafkaTemplate<String, String> outputKafkaTemplate) {
        this.outputKafkaTemplate = outputKafkaTemplate;
    }

    public void processEvent(Event event) {
        LOGGER.info("Received event : {}", event);
        outputKafkaTemplate.send(new GenericMessage<>(new Event(event.getPayload())));
    }

}