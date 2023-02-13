package org.opendatadiscovery.adapters.spark.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
@Getter
@ToString
public class ExecutionPayload {
    private final String applicationName;
    private final String hostName;
    private final OffsetDateTime jobStartTime;

    @Setter
    private long jobEndTime = 0;

    private Map<Object, Object> metadata = new HashMap<>();

    @Setter
    private String errorMessage;

    public void appendMetadata(final Map<Object, Object> chunk) {
        metadata.putAll(chunk);
    }
}
