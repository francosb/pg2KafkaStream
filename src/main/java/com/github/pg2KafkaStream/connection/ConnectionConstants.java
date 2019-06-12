package com.github.pg2KafkaStream.connection;


import java.util.concurrent.TimeUnit;

public interface ConnectionConstants {

    //  JDBC connection constants for properties
    String REPLICATION_PROPERTY = "database";
    String PREFER_QUERY_MODE_PROPERTY = "simple";
    String DEFAULT_PORT = "5432";
    String MIN_SERVER_VERSION = "9.6";
    String DEFAULT_SSL_MODE = "disable";

    // Postgres replication constants
    int DEFAULT_STATUS_INTERVAL_VALUE = 20;
    TimeUnit DEFAULT_STATUS_INTERVAL_TIME_UNIT = TimeUnit.SECONDS;
    boolean DEFAULT_INCLUDE_XIDS = true;
    boolean DEFAULT_INCLUDE_TIMESTAMP = true;
    boolean DEFAULT_INCLUDE_LSN = true;
    boolean DEFAULT_INCLUDE_TYPES = false;
    String DEFAULT_OUTPUT_PLUGIN = "wal2json";
    int DEFAULT_UPDATE_IDLE_SLOT_INTERVAL = 300;
    int DEFAULT_EXISTING_PROCESS_RETRY_LIMIT = 30;
    int DEFAULT_EXISTING_PROCESS_RETRY_SLEEP_SECONDS = 30;

    // SLOT constants
    String ALL_EVENT = "all";




}
