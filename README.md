# Flink State Inspector

Auto-discovery tool for inspecting Apache Flink savepoint and checkpoint state. Select an environment, pick a checkpoint, choose an operator, and browse the state. No custom reader classes required.

## Why

Flink's State Processor API requires you to write a `KeyedStateReaderFunction` for every operator, with exact state descriptors matching your production job. That means maintaining a parallel codebase just to debug state. This tool eliminates that by auto-discovering operators, state names, and serializer metadata directly from the savepoint's `_metadata` file.

Compatible with savepoints from Flink 1.x and 2.x (the State Processor API maintains backward compatibility across versions).

## Features

- **Web UI**: React dashboard for browsing, inspecting, and diffing state with guided navigation
- **Auto-discovery**: reads the `_metadata` file to find all operators, state names, and types automatically
- **Generic deserialization**: handles built-in Flink types (String, Long, POJO, Avro, Protobuf) without domain-specific code
- **Pluggable storage**: abstract `StorageConnector` class with built-in support for local filesystem, S3, GCS, and Docker containers
- **State diff**: select two checkpoints and compare state side-by-side to see what changed
- **CLI**: full command-line interface for scripting and automation

## Web UI

The primary interface is a React web dashboard served by the built-in Javalin server:

```bash
java -jar target/flink-state-inspector.jar serve --port 9741
```

Open http://localhost:9741 in your browser. The UI guides you through:

1. **Browse**: select an environment and storage bucket, or enter a path directly. The tool discovers all available checkpoints and savepoints, displayed in a sortable, filterable table.
2. **Inspect**: pick a checkpoint, choose an operator from the auto-discovered list, and browse the state entries. Expandable rows show full JSON state values. Supports key filtering and keys-only mode.
3. **Diff**: select two checkpoints to compare. The tool shows added, removed, and modified state entries across operators.
4. **Docs**: in-app documentation and API reference.

## CLI

For scripting and automation, the same functionality is available via CLI commands:

```bash
# Build the fat JAR
mvn package -DskipTests

# List checkpoints at a path
java -jar target/flink-state-inspector.jar list /path/to/checkpoints

# Inspect state in a savepoint (auto-discovers all operators)
java -jar target/flink-state-inspector.jar inspect /path/to/savepoint

# Inspect a specific operator
java -jar target/flink-state-inspector.jar inspect /path/to/savepoint --operator my-operator-uid

# Diff two savepoints
java -jar target/flink-state-inspector.jar diff /path/to/savepoint-1 /path/to/savepoint-2

# Interactive terminal browser
java -jar target/flink-state-inspector.jar browse /path/to/checkpoints
```

### CLI Options

```
inspect:
  --operator, -o    Filter by operator UID
  --state, -s       Filter by state name
  --keys-only, -k   Show only state keys, not values
  --key-filter      Filter entries by key pattern
  --json            Output as raw JSON
  --output, -O      Export results to file

diff:
  --operator, -o    Filter by operator UID
  --keys-only, -k   Compare keys only, ignore values

list:
  --limit, -n       Max entries to show (default: 20)
```

## Storage Connectors

The tool reads checkpoint data from multiple storage backends. The connector is selected automatically by URI scheme:

| Scheme | Connector | Example |
|--------|-----------|---------|
| (none) | Local filesystem | `/data/flink/checkpoints` |
| `s3://` | AWS S3 | `s3://my-bucket/flink/checkpoints` |
| `gs://` | Google Cloud Storage | `gs://my-bucket/flink/checkpoints` |
| `docker://` | Docker container | `docker://flink-taskmanager/opt/flink/checkpoints` |

### Docker Connector

For local development with Docker Compose, the Docker connector reads checkpoints directly from inside running containers:

```bash
java -jar target/flink-state-inspector.jar list docker://flink-taskmanager/opt/flink/checkpoints
```

### Custom Connectors

Extend the `StorageConnector` abstract class to add support for additional backends (HDFS, Azure Blob Storage, MinIO, etc.):

```java
public class HdfsStorageConnector extends StorageConnector {

    @Override
    public String scheme() {
        return "hdfs";
    }

    @Override
    public void initialize(Map<String, String> config) {
        // set up HDFS client
    }

    // implement remaining abstract methods...
}
```

Register your connector in `StorageConnectorFactory.resolveConnector()`.

## Architecture

```mermaid
graph TD
    WebUI[React Web UI] --> API[REST API / Javalin]
    CLI[CLI / picocli] --> Commands
    API --> Commands

    Commands --> Discovery[Operator Discovery]
    Commands --> Reader[Generic State Reader]
    Commands --> Diff[Diff Engine]

    Discovery --> Storage[StorageConnector]
    Reader --> Storage

    Storage --> Local[Local FS]
    Storage --> S3[AWS S3]
    Storage --> GCS[Google Cloud Storage]
    Storage --> Docker[Docker Container]
    Storage --> Custom[Your Connector]
```

## Custom Types

For jobs using custom POJO or Avro types, add your application JAR to the classpath:

```bash
java -cp flink-state-inspector.jar:my-flink-app.jar \
  io.flinkstate.inspector.FlinkStateInspector inspect /path/to/savepoint
```

Built-in Flink types (String, Long, Integer, Maps, Lists) work without additional JARs.

## Building

Requires JDK 17+ and Node.js 18+ (for the web UI).

```bash
mvn verify          # compile + test (backend)
cd ui && npm run build   # build web UI
mvn package         # build fat JAR with UI assets
```

## Project Status

Under active development. See the [issue tracker](https://github.com/atomicdragonranch/flink-state-inspection-tool/issues) for the roadmap.

**Implemented:**
- Storage connector abstraction with Local and Docker connectors
- CLI framework with all commands
- Checkpoint discovery and listing

**In progress:**
- Operator auto-discovery from savepoint metadata (#1)
- Generic state reader (#2)
- Web UI port (#12)
- S3 and GCS connectors (#3, #4)

## License

[MIT](LICENSE)
