package org.opendatadiscovery.adapters.spark.utils;

import lombok.Builder;
import lombok.Data;
import org.opendatadiscovery.oddrn.annotation.PathField;
import org.opendatadiscovery.oddrn.model.OddrnPath;

@Data
@Builder
public class AwsS3Path implements OddrnPath {
    @PathField
    private final String bucket;

    @PathField(dependency = "bucket", prefix = "keys")
    private final String key;

    @Override
    public String prefix() {
        return "//s3/cloud/aws";
    }
}
