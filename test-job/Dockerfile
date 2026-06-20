FROM flink:1.20-java17

RUN mkdir -p /tmp/flink-checkpoints /tmp/flink-savepoints && \
    chown -R flink:flink /tmp/flink-checkpoints /tmp/flink-savepoints

COPY target/state-inspector-test-job-1.0.0-SNAPSHOT.jar /opt/flink/usrlib/state-inspector-test-job.jar
