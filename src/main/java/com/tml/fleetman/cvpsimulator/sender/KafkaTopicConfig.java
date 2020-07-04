package com.tml.fleetman.cvpsimulator.sender;

import java.util.Map;
import java.util.HashMap;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * @author Pallavi Shetty
 * @since May 2020
 */

@Configuration
public class KafkaTopicConfig {

    @Value(value = "${kafka.bootstrapServers}")
    private String bootstrapAddress;


    @Value(value = "${kafka.consumer.topics.telemetryTopic}")
    private String cvpTopicName;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

   

    @Bean
    public NewTopic topic4() {
        return new NewTopic(cvpTopicName, 1, (short) 1);
    }
}
