package com.jpmc.midascore.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic traderUpdatesTopic() {
        return new NewTopic("trader-updates", 1, (short) 1);
    }
}
