package com.github.pg2KafkaStream.connection;

import java.sql.SQLException;

public interface ConnectionInterface extends AutoCloseable {

    /**
     * Checks whether this connection is open or not
     *
     * @return {@code true} if this connection is open, {@code false} otherwise
     * @throws SQLException if anything unexpected fails
     */
    public boolean isConnected() throws SQLException;


}