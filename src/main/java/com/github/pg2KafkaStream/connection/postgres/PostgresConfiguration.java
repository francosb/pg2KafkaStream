package com.github.pg2KafkaStream.connection.postgres;

import org.postgresql.PGProperty;

import java.util.Properties;

import static com.github.pg2KafkaStream.connection.ConnectionConstants.*;

public interface PostgresConfiguration {


    default String getPort() {
        return DEFAULT_PORT;
    }

    default String getMinServerVersion() {
        return MIN_SERVER_VERSION;
    }

    default String getReplication() {
        return REPLICATION_PROPERTY;
    }

    default String getQueryMode() {
        return PREFER_QUERY_MODE_PROPERTY;
    }

    default String getUrl() {
        return String.format("jdbc:postgresql://%s:%s/%s", getHost(),
                getPort(), getDatabase());
    }

    default Properties getReplicationProperties() {
        Properties properties = getQueryConnectionProperties();
        PGProperty.PREFER_QUERY_MODE.set(properties, getQueryMode());
        PGProperty.REPLICATION.set(properties, getReplication());
        return properties;
    }

    default Properties getQueryConnectionProperties() {
        Properties properties = new Properties();
        PGProperty.USER.set(properties, getUsername());
        PGProperty.PASSWORD.set(properties, getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties,
                getMinServerVersion());
        PGProperty.REPLICATION.set(properties, getReplication());
        PGProperty.PREFER_QUERY_MODE.set(properties, getQueryMode());
        PGProperty.TCP_KEEP_ALIVE.set(properties, true);
        PGProperty.CONNECT_TIMEOUT.set(properties, 0);
        PGProperty.SOCKET_TIMEOUT.set(properties, 0);
        PGProperty.CANCEL_SIGNAL_TIMEOUT.set(properties, 3600);
        return properties;
    }

    String getHost();

    String getDatabase();

    String getUsername();

    String getPassword();

}
