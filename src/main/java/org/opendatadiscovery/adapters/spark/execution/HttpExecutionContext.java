package org.opendatadiscovery.adapters.spark.execution;

import lombok.extern.slf4j.Slf4j;
import org.opendatadiscovery.adapters.spark.dto.ExecutionPayload;
import org.opendatadiscovery.client.ApiClient;
import org.opendatadiscovery.client.api.OpenDataDiscoveryIngestionApi;
import org.opendatadiscovery.client.model.DataEntityList;

import java.time.Duration;

@Slf4j
public class HttpExecutionContext extends AbstractExecutionContext {
    private final OpenDataDiscoveryIngestionApi apiClient;

    public HttpExecutionContext(final ExecutionPayload executionPayload,
                                final String oddPlatformUrl) {
        super(executionPayload);
        this.apiClient = new OpenDataDiscoveryIngestionApi(new ApiClient().setBasePath(oddPlatformUrl));
    }

    @Override
    public void reportApplicationEnd() {
        final DataEntityList dataEntityList = buildODDModels();

        apiClient.postDataEntityListWithHttpInfo(dataEntityList)
            .doOnError(t -> log.error("Couldn't send payload to the ODD Platform", t))
            .block(Duration.ofSeconds(30));
    }
}