package io.flinkstate.inspector.commands;

import picocli.CommandLine;

import java.util.LinkedHashMap;
import java.util.Map;

public class S3Options {

    @CommandLine.Option(names = "--aws-region", description = "AWS region (default: AWS_DEFAULT_REGION or us-east-1)")
    String awsRegion;

    @CommandLine.Option(names = "--aws-endpoint", description = "Custom S3 endpoint (for LocalStack or MinIO)")
    String awsEndpoint;

    @CommandLine.Option(names = "--aws-profile", description = "AWS profile name")
    String awsProfile;

    @CommandLine.Option(names = "--s3-path-style", description = "Use path-style S3 access (required for LocalStack/MinIO)")
    boolean pathStyleAccess;

    public Map<String, String> toConfigMap() {
        Map<String, String> config = new LinkedHashMap<>();
        if (awsRegion != null) config.put("aws.region", awsRegion);
        if (awsEndpoint != null) config.put("aws.endpoint", awsEndpoint);
        if (awsProfile != null) config.put("aws.profile", awsProfile);
        if (pathStyleAccess) config.put("aws.path-style-access", "true");
        return config;
    }
}
