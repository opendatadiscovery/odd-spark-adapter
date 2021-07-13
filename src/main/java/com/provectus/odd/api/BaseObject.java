package com.provectus.odd.api;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class BaseObject {
    String oddrn;
    String name;
    String description;
    String owner;
    String type; // TODO enum
    Map<String, String> metadata = new HashMap<>();

    public BaseObject() {
    }
}
