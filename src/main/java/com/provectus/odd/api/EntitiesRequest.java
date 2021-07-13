package com.provectus.odd.api;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Getter
public class EntitiesRequest {
    String data_source_oddrn;
    List<DataEntity> items;

    public EntitiesRequest(String aDataSourceOddrn, List<DataEntity> aItems) {
        this.data_source_oddrn = aDataSourceOddrn;
        this.items = aItems;
    }

    public EntitiesRequest(String aDataSourceOddrn, DataEntity aEntity) {
        this(aDataSourceOddrn, Collections.singletonList(aEntity));
    }
}
