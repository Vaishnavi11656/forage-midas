package com.jpmc.midascore;

import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092"
})
public class TaskFourTests {

    static final Logger logger = LoggerFactory.getLogger(TaskFourTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private UserRepository userRepository; // 👈 Needed to read balances

    @Test
    void task_four_verifier() throws InterruptedException {

        // Step 1: Create users
        userPopulator.populate();

        // Step 2: Load transactions from file
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");

        // Step 3: Send each transaction to Kafka
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }

        // Step 4: Wait for Kafka listener to process everything
        Thread.sleep(3000);

        // Step 5: Print final balances (THIS is what you need)
        System.out.println("\n===== FINAL USER BALANCES =====");
        userRepository.findAll().forEach(user ->
                System.out.println(user.getName() + " -> " + user.getBalance())
        );

        // Step 6: Small delay so logs don't disappear instantly
        Thread.sleep(5000);
    }
}
