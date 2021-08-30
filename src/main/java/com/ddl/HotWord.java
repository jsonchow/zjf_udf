package com.ddl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HotWord {
    @JsonProperty("id")
    private String id;
    @JsonProperty("producer_type")
    private String producer_type;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getProducer_type() {
        return producer_type;
    }

    public void setProducer_type(String producer_type) {
        this.producer_type = producer_type;
    }
}