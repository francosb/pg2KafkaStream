package com.github.pg2KafkaStream.kafka;

import com.github.pg2KafkaStream.connection.postgres.PostgresJdbcConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static com.github.pg2KafkaStream.kafka.KafkaConstants.DEFAULT_FLUSH_INTERVAL;


public class KafkaProducerHandler {
    private final KafkaProducer<String, String> kafkaProducer;
    private final PostgresJdbcConnection postgresConnectorInput;
    private int count = 0;

    public KafkaProducerHandler(KafkaConfiguration kafkaConfiguration, PostgresJdbcConnection postgresConnectorInput) {
        this.postgresConnectorInput = postgresConnectorInput;
        this.kafkaProducer = new KafkaProducer<>(kafkaConfiguration.getKafkaOptions());
    }

    void stopProducer() {
        kafkaProducer.close();
    }

    /**
     * Create and send {@link ProducerRecord} to kafka implementing callback
     * {@link KafkaProducerCallback(PostgresJdbcConnection)}.
     *
     * Flush kafka producer after a default iteration interval.
     * @param line
     * @param topicName
     */
    void pushToKafka(String line, String topicName) {
        ProducerRecord<String, String> data = new ProducerRecord<>(topicName,
                line + '\n');
        kafkaProducer.send(data, new KafkaProducerCallback(postgresConnectorInput));
        if (this.count == DEFAULT_FLUSH_INTERVAL) {
            kafkaProducer.flush();
            this.count = 0;
        }
        count++;
    }

    public PostgresJdbcConnection getPostgresJdbcConnection() {
        return postgresConnectorInput;
    }
}
