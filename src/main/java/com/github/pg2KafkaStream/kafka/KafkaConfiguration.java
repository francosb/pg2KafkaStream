package com.github.pg2KafkaStream.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public interface KafkaConfiguration {

    String getBrokers();

    String getTopics();

    default String getBatchSizeConfig() {
        return KafkaConstants.BATCH_SIZE_CONFIG;
    }

    default Properties getKafkaOptions() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, getBatchSizeConfig());
        return props;
    }


}
