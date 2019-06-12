package com.github.pg2KafkaStream.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public abstract class JdbcConnection {

    @FunctionalInterface
    public interface ConnectionFactory {
        /**
         * Establish a connection to the database denoted by the given configuration.
         *
         * @param config the configuration with JDBC connection information
         * @return the JDBC connection; may not be null
         * @throws SQLException if there is an error connecting to the database
         */
        Connection connect(String url, Properties config) throws SQLException;
    }

    public abstract Connection getConnection(String url, Properties config) throws SQLException;

}
