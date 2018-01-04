package com.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class MyReportListener {

    private final MyService myService;

    @Autowired
    public MyReportListener(MyService myService) {
        this.myService = myService;
    }

    @KafkaListener(topics = "listenerTopic")
    @Transactional
    public void onEvent(Event event) {
        myService.processEvent(event);
    }
}