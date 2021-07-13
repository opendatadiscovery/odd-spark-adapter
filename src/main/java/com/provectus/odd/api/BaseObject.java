package com.provectus.odd.api;

import lombok.Getter;

@Getter
public class BaseObject {
    String oddrn;
    String name;
    String description;
    String owner;
    String type; // TODO enum
    Metadata metadata = new Metadata();

    public BaseObject() {
    }
}
