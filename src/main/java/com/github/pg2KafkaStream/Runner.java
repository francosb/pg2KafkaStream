package com.github.pg2KafkaStream;

import com.github.pg2KafkaStream.connection.postgres.PostgresConfiguration;
import com.github.pg2KafkaStream.connection.postgres.ReplicationConfiguration;
import com.github.pg2KafkaStream.kafka.KafkaConfiguration;
import com.github.pg2KafkaStream.kafka.PgStreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.Optional;

public class Runner implements
        PostgresConfiguration,
        ReplicationConfiguration,
        KafkaConfiguration,
        Runnable {

    private static final Logger logger = LoggerFactory.getLogger(
            Runner.class);


    public static Optional<Runner> initialize(final String[] args) {
        final Runner runner = new Runner();
        new CommandLine(runner).parseArgs(args);
        if (runner.usageHelpRequested) {
            CommandLine.usage(new Runner(), System.out);
            return Optional.empty();
        } else {
            return Optional.of(runner);
        }
    }

    public static void main(final String[] args) {
        initialize(args).ifPresent(Runner::run);
    }

    @CommandLine.Option(
            names = {"--pgport"},
            description = "Postrgres database server port",
            defaultValue = "5432"
    )
    private String pgPort;

    @CommandLine.Option(
            names = {"--pghost"},
            description = "Postrgres database server host",
            required = true
    )
    private String pgHost;

    @CommandLine.Option(
            names = {"--pguser"},
            description = "Postrgres database username",
            required = true
    )
    private String pgUser;

    @CommandLine.Option(
            names = {"--pgpassword"},
            description = "Postrgres database password",
            required = true
    )
    private String pgPassword;

    @CommandLine.Option(
            names = {"--pgdatabase"},
            description = "Postrgres database name",
            required = true
    )
    private String pgDatabase;

    @CommandLine.Option(
            names = {"--slotname"},
            description = "Postrgres replication slot name",
            required = true
    )

    private String slotName;

    @CommandLine.Option(
            names = {"--kind"},
            description = "Kind of WAL event to process, "
                    + "availables options:"
                    + "insert, update, delete, all.",
            required = true
    )
    private String kind;

    @CommandLine.Option(
            names = {"--tables"},
            description = "Include only rows from the specified tables",
            required = true
    )
    private String tables;

    @CommandLine.Option(
            names = {"--brokers"},
            description = "Kafka brokers list separate by comma",
            required = true
    )
    private String brokers;

    @CommandLine.Option(
            names = {"--topics"},
            description = "Kafka topics names",
            required = true
    )
    private String topics;


    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true,
            description = "Display this message")
    private boolean usageHelpRequested;

    @Override
    public String getPort() {
        return pgPort;
    }

    @Override
    public String getHost() {
        return pgHost;
    }

    @Override
    public String getDatabase() {
        return pgDatabase;
    }

    @Override
    public String getUsername() {
        return pgUser;
    }

    @Override
    public String getPassword() {
        return pgPassword;
    }

    @Override
    public String getSlotName() {
        return slotName;
    }

    @Override
    public String getKind() {
        return kind;
    }

    @Override
    public String getTables() {
        return tables;
    }

    @Override
    public String getBrokers() {
        return brokers;
    }

    @Override
    public String getTopics() {
        return topics;
    }

    @Override
    public void run() {
        try {
            new PgStreamProducer(
                    this,
                    this,
                    this
            ).runLoop();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
