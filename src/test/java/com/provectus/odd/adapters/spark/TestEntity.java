package com.provectus.odd.adapters.spark;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;

public class TestEntity {
    String stuff = "zzz";

    @JsonGetter("thing")
    public String getStuff() {
        return stuff;
    }

    @JsonSetter("thing")
    public void setStuff(String stuff) {
        this.stuff = stuff;
    }
}
