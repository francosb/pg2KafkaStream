package com.github.pg2KafkaStream.kafka;

import com.github.pg2KafkaStream.connection.ConnectionConstants;
import com.github.pg2KafkaStream.connection.postgres.PostgresConfiguration;
import com.github.pg2KafkaStream.connection.postgres.PostgresJdbcConnection;
import com.github.pg2KafkaStream.connection.postgres.ReplicationConfiguration;
import com.github.pg2KafkaStream.models.SlotMessage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class PgStreamProducer {
    private final PostgresConfiguration postgresConfiguration;
    private final ReplicationConfiguration replicationConfiguration;
    private final KafkaConfiguration kafkaConfiguration;
    private static final String recoveryModeSqlState = "57P03";
    private static final int recoveryModeSleepMillis = 5000;
    private final HashMap<String, String> topicTablesMap;
    private Gson gson;

    private long lastFlushedTime;
    private static final Logger logger = LoggerFactory.getLogger(PgStreamProducer.class);

    public PgStreamProducer(final PostgresConfiguration postgresConfigurationInput,
                            final ReplicationConfiguration replicationConfigurationInput,
                            final KafkaConfiguration kafkaConfiguration) {
        this.postgresConfiguration = postgresConfigurationInput;
        this.replicationConfiguration = replicationConfigurationInput;
        this.kafkaConfiguration = kafkaConfiguration;
        this.gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        this.topicTablesMap = createTableTopicHashMap(this.replicationConfiguration, this.kafkaConfiguration);
    }

    /**
     * Runs  continuously in a loop.
     */
    public void runLoop() throws Exception {
        while (true) {
            sendToKafka();
        }
    }

    void sendToKafka() throws Exception {
        logger.info("Consuming from slot {}", replicationConfiguration
                .getSlotName());
        KafkaProducerHandler kafkaProducerHandler;

        try (PostgresJdbcConnection postgresJdbcConnection = createPostgresConnector(postgresConfiguration, replicationConfiguration)) {
            resetIdleCounter();
            kafkaProducerHandler = createKafkaProducer(kafkaConfiguration, postgresJdbcConnection);
            logger.info("Consuming from slot {}", replicationConfiguration.getSlotName());
            while (true) {
                sendToKafkaHelper(kafkaProducerHandler);
            }
        } catch (SQLException sqlException) {
            logger.error("Received the following error pertaining to the "
                    + "replication stream, reattempting...", sqlException);
            if (sqlException.getSQLState().equals(recoveryModeSqlState)) {
                logger.info("Sleeping for five seconds");
                try {
                    Thread.sleep(recoveryModeSleepMillis);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted while sleeping", ie);

                }
            }
        } catch (Exception e) {
            logger.error("Received exception of type {}", e.getClass().toString(), e);
            throw new Exception(String.format("Received exception of type %s, %s",e.getClass().toString(), e));
        }
    }

    void sendToKafkaHelper(final KafkaProducerHandler kafkaProducerHandler) throws SQLException {

        ByteBuffer msg = kafkaProducerHandler.getPostgresJdbcConnection().readPending();
        if (msg != null) {
            processByteBuffer(msg, kafkaProducerHandler);
        } else if (System.currentTimeMillis() - lastFlushedTime
                > TimeUnit.SECONDS.toMillis(replicationConfiguration
                .getUpdateIdleSlotInterval())) {
            LogSequenceNumber lsn = kafkaProducerHandler.getPostgresJdbcConnection().getLastReceivedLsn();
            msg = kafkaProducerHandler.getPostgresJdbcConnection().readPending();
            if (msg != null) {
                processByteBuffer(msg, kafkaProducerHandler);
            }
            logger.info("Fast forwarding stream lsn to {} due to stream "
                    + "inactivity", lsn.toString());
            kafkaProducerHandler.getPostgresJdbcConnection().setStreamLsn(lsn);
            resetIdleCounter();
        }
    }

    /**
     * Create an {@link SlotMessage} object named slotMessage, parsing from JSON String geted from
     * ByteBuffer message, using Gson.
     *
     * Verify if slotMessage has a change event, in case true, get the change from slotMessage and
     * compare it kind with the kind specified on {@link ReplicationConfiguration} replicationConfiguration
     * to filter incoming changes records. An change kind can be an insert, an update or a delete.
     *
     * After kind comparison, verify if tableName attribute on slotMessage exist on table-topic HashMap,
     * in case true, slotMessage is parsed to JSON and passed to function
     * {@link KafkaProducerHandler#pushToKafka(String, String)} for send the record to table
     * specific kafka topic.
     *
     * @param msg
     * @param kafkaProducerHandler
     */
    void processByteBuffer(final ByteBuffer msg, final KafkaProducerHandler kafkaProducerHandler) {
        logger.debug("Processing chunk from wal");
        int offset = msg.arrayOffset();
        byte[] source = msg.array();
        int length = source.length - offset;
        SlotMessage slotMessage = gson.fromJson(new String(source, offset, length), SlotMessage.class);
        if (slotMessage.getChange().size() > 0) {
            String tableName = slotMessage.getChange().get(0).getTable();
            if (replicationConfiguration.getKind().equals(ConnectionConstants.ALL_EVENT)) {
                if (topicTablesMap.containsKey(tableName))
                    kafkaProducerHandler.pushToKafka(gson.toJson(slotMessage), topicTablesMap.get(tableName));
            } else {
                if (slotMessage.getChange().get(0).getKind().equals(replicationConfiguration.getKind())) {
                    if (topicTablesMap.containsKey(tableName)) {
                        kafkaProducerHandler.pushToKafka(gson.toJson(slotMessage), topicTablesMap.get(tableName));
                    }
                }
            }
        }
    }

    public void resetIdleCounter() {
        lastFlushedTime = System.currentTimeMillis();
    }

    PostgresJdbcConnection createPostgresConnector(final PostgresConfiguration pc,
                                                   final ReplicationConfiguration rc) throws SQLException {
        return new PostgresJdbcConnection(pc, rc);
    }

    KafkaProducerHandler createKafkaProducer(final KafkaConfiguration kafkaConfiguration,
                                             final PostgresJdbcConnection postgresJdbcConnection) {
        return new KafkaProducerHandler(kafkaConfiguration, postgresJdbcConnection);
    }

    /**
     * Create a string Hashmap to match the table name with its corresponding
     * kafka topic.
     * @param replicationConfiguration
     * @param kafkaConfiguration
     * @return  HashMap<String, String> hashMap
     */
    HashMap<String, String> createTableTopicHashMap(final ReplicationConfiguration replicationConfiguration,
                                                    final KafkaConfiguration kafkaConfiguration) {
        HashMap<String, String> hashMap = new HashMap<>();
        ArrayList<String> tablesList = new ArrayList<>(
                Arrays.asList(replicationConfiguration.getTables().split(",")));
        ArrayList<String> topicsList = new ArrayList<>(
                Arrays.asList(kafkaConfiguration.getTopics().split(",")));
        for (int i = 0; i < tablesList.size(); i++) hashMap.put(tablesList.get(i), topicsList.get(i));

        return hashMap;
    }

}
