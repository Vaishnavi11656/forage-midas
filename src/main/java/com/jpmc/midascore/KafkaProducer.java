package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final String topic = "trader-updates";

    @Autowired
    public KafkaProducer(KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String transactionLine) {
        String[] parts = transactionLine.split(", ");
        Transaction transaction = new Transaction(
                Long.parseLong(parts[0]),
                Long.parseLong(parts[1]),
                Float.parseFloat(parts[2])
        );
        kafkaTemplate.send(topic, transaction);
    }
}
