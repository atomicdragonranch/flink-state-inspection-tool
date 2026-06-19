# Contributing

## Development Setup

- JDK 17+ required
- Maven 3.8+
- Docker (for integration tests and Docker connector)

## Running Tests

```bash
# Unit tests only
mvn test

# Unit + integration tests (requires Docker)
mvn verify -Pintegration
```

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` new feature
- `fix:` bug fix
- `chore:` build, deps, CI changes
- `docs:` documentation only
- `refactor:` code change that neither fixes a bug nor adds a feature
- `test:` adding or updating tests

## Test Style

JUnit 5 + AssertJ. Use explicit section comments in all test methods:

```java
@Test
void myTest() {
    // Arrange
    ...

    // Act
    ...

    // Assert
    ...
}
```

## Storage Connectors

New storage backends extend `StorageConnector` and register in `StorageConnectorFactory`. Follow the existing `DockerStorageConnector` or `S3StorageConnector` as reference implementations.
