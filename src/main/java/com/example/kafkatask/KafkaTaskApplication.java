package com.example.kafkatask;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
//@EntityScan("com.example.kafkatask.entity")
//@EnableJpaRepositories("com.example.kafkatask.repository") // Add this line
//@ComponentScan(basePackages = "com.example.kafkatask")
public class KafkaTaskApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaTaskApplication.class, args);
    }
}
