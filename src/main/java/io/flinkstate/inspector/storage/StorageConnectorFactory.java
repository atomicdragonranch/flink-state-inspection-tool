package io.flinkstate.inspector.storage;

import java.util.Collections;
import java.util.Map;

public class StorageConnectorFactory {

    private StorageConnectorFactory() {
    }

    public static StorageConnector create(String path) {
        return create(path, Collections.emptyMap());
    }

    public static StorageConnector create(String path, Map<String, String> config) {
        StorageConnector connector = resolveConnector(path);
        connector.initialize(config);
        return connector;
    }

    private static StorageConnector resolveConnector(String path) {
        if (path.startsWith("s3://") || path.startsWith("s3a://")) {
            return new S3StorageConnector();
        }
        if (path.startsWith("gs://")) {
            return new GcsStorageConnector();
        }
        if (path.startsWith("docker://")) {
            return new DockerStorageConnector();
        }
        return new LocalStorageConnector();
    }
}
