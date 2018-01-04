package com.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class Event {

    private String payload;

    @JsonCreator
    public Event(@JsonProperty("payload") String payload) {
        this.payload = payload;
    }

    public String getPayload() {
        return payload;
    }
}
