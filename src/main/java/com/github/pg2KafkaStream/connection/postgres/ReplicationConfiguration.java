package com.github.pg2KafkaStream.connection.postgres;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.github.pg2KafkaStream.connection.ConnectionConstants.*;

public interface ReplicationConfiguration {

    String getSlotName();

    String getKind();

    String getTables();


    default int getStatusIntervalValue() {
        return DEFAULT_STATUS_INTERVAL_VALUE;
    }

    default TimeUnit getStatusIntervalTimeUnit() {
        return DEFAULT_STATUS_INTERVAL_TIME_UNIT;
    }

    default boolean getIncludeXids() {
        return DEFAULT_INCLUDE_XIDS;
    }

    default boolean getIncludeTypes() {
        return DEFAULT_INCLUDE_TYPES;
    }

    default boolean getIncludeTimestamp() {
        return DEFAULT_INCLUDE_TIMESTAMP;
    }

    default boolean getIncludeLsn() {
        return DEFAULT_INCLUDE_LSN;
    }

    default String getOutputPlugin() {
        return DEFAULT_OUTPUT_PLUGIN;
    }

    default Properties getSlotOptions() {
        Properties properties = new Properties();
        properties.setProperty("include-xids", String.valueOf(getIncludeXids()));
        properties.setProperty("include-types", String.valueOf(getIncludeTypes()));
        properties.setProperty("include-timestamp", String.valueOf(getIncludeTimestamp()));
        properties.setProperty("include-lsn", String.valueOf(getIncludeLsn()));
        return properties;
    }

    default int getUpdateIdleSlotInterval() {
        return DEFAULT_UPDATE_IDLE_SLOT_INTERVAL;
    }

    default int getExisitingProcessRetryLimit() {
        return DEFAULT_EXISTING_PROCESS_RETRY_LIMIT;
    }

    default int getExistingProcessRetrySleepSeconds() {
        return DEFAULT_EXISTING_PROCESS_RETRY_SLEEP_SECONDS;
    }

}
