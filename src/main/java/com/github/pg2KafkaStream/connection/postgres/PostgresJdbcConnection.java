package com.github.pg2KafkaStream.connection.postgres;

import com.github.pg2KafkaStream.connection.ConnectionInterface;
import com.github.pg2KafkaStream.connection.JdbcConnection;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class PostgresJdbcConnection extends JdbcConnection implements ConnectionInterface {
    private static final String currentlyRunningProcessOnSlotSqlState = "55006";

    private final Connection streamingConnection;
    private final PGReplicationStream pgReplicationStream;
    private Connection conn;

    private static final Logger logger =
            LoggerFactory.getLogger(PostgresJdbcConnection.class);

    /**
     * Initializes {@link #streamingConnection} for opening up {@link #pgReplicationStream}
     *
     * @param postgresConf
     * @param replicationConf
     * @throws SQLException
     */
    public PostgresJdbcConnection(final PostgresConfiguration postgresConf,
                                  final ReplicationConfiguration replicationConf) throws SQLException {
        streamingConnection = getConnection(postgresConf.getUrl(), postgresConf.getReplicationProperties());
        PGConnection pgConnection = streamingConnection.unwrap(PGConnection.class);
        PGReplicationConnection pgReplicationConnection = pgConnection.getReplicationAPI();
        pgReplicationStream = getPgReplicationStream(replicationConf, pgReplicationConnection);

    }

    private final ConnectionFactory factory = DriverManager::getConnection;

    public synchronized boolean isConnected() throws SQLException {
        if (conn == null) {
            return false;
        }
        return !conn.isClosed();
    }

    public synchronized Connection getConnection(String url, Properties config) throws SQLException {
        if (!isConnected()) {
            conn = factory.connect(url, config);
            if (!isConnected()) {
                throw new SQLException("Unable to obtain a JDBC connection");
            }
        }
        return conn;
    }

    public ByteBuffer readPending() throws SQLException {
        return pgReplicationStream.readPending();
    }

    /**
     * Perform  {@link PGReplicationStream#setAppliedLSN(LogSequenceNumber)} and
     * {@link PGReplicationStream#setFlushedLSN(LogSequenceNumber)} function to send to backend
     * on next update status iteration the flushed LSN position.
     *
     * @param lsn not null location of the last WAL applied in the standby.
     */
    public void setStreamLsn(final LogSequenceNumber lsn) {
        pgReplicationStream.setAppliedLSN(lsn);
        pgReplicationStream.setFlushedLSN(lsn);
    }

    public LogSequenceNumber getLastReceivedLsn() {
        return pgReplicationStream.getLastReceiveLSN();
    }

    @Override
    public void close() {
        if (pgReplicationStream != null) {
            try {
                if (!pgReplicationStream.isClosed()) {
                    pgReplicationStream.forceUpdateStatus();
                    pgReplicationStream.close();
                }
            } catch (SQLException sqlException) {
                logger.error("Unable to close replication stream",
                        sqlException);
            }
        }
        if (streamingConnection != null) {
            try {
                streamingConnection.close();
            } catch (SQLException sqlException) {
                logger.error("Unable to close postgres streaming connection",
                        sqlException);
            }
        }
    }

    /**
     * Retry initializing the stream according to settings in
     * ReplicationConfiguration. Do this because only one PID
     * may consume from a slot at once.
     *
     * @param replicationConfiguration
     * @param pgReplicationConnection
     * @return {@link PGReplicationStream} pgRepStream
     * @throws SQLException
     */
    PGReplicationStream getPgReplicationStream(
            final ReplicationConfiguration replicationConfiguration,
            final PGReplicationConnection pgReplicationConnection)
            throws SQLException {
        boolean listening = false;
        int tries = replicationConfiguration.getExisitingProcessRetryLimit();
        PGReplicationStream pgRepStream = null;
        while (!listening && tries > 0) {
            pgRepStream = getPgReplicationStreamHelper(replicationConfiguration, pgReplicationConnection);
            listening = true;

        }
        return pgRepStream;
    }

    PGReplicationStream getPgReplicationStreamHelper(
            final ReplicationConfiguration replicationConfiguration,
            final PGReplicationConnection pgReplicationConnection)
            throws SQLException {
        return pgReplicationConnection
                .replicationStream()
                .logical()
                .withStatusInterval(replicationConfiguration
                                .getStatusIntervalValue(),
                        replicationConfiguration.getStatusIntervalTimeUnit())
                .withSlotOptions(replicationConfiguration.getSlotOptions())
                .withSlotName(replicationConfiguration.getSlotName()).start();
    }

}
