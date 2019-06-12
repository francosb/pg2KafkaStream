package com.github.pg2KafkaStream.kafka;

import com.github.pg2KafkaStream.connection.postgres.PostgresJdbcConnection;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerCallback implements Callback {

    private static final Logger logger =
            LoggerFactory.getLogger(KafkaProducerCallback.class);

    private final LogSequenceNumber lsn;
    private final PostgresJdbcConnection postgresConnector;

    protected KafkaProducerCallback(final PostgresJdbcConnection postgresConnectorInput) {
        this.postgresConnector = postgresConnectorInput;
        this.lsn = postgresConnector.getLastReceivedLsn();
    }

    /**
     * Kafka callback used to flush last processed lsn after put record
     * on kafka using function {@link PostgresJdbcConnection#setStreamLsn(LogSequenceNumber)}.
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e == null) {
                postgresConnector.setStreamLsn(lsn);
            } else {
                logger.error("Failed to put record with postgres sequence number {}", lsn);
            }
    }
}
