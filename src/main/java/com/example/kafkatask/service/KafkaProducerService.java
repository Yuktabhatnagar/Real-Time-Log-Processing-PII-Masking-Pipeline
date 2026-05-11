package com.example.kafkatask.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

/**
 * The type Kafka service.
 */
@Log4j2
@Service
public class KafkaProducerService {
    /**
     * Bean of KafkaTemplate.
     */
    private KafkaTemplate kafkaTemplate;
    /**
     * Bean of ObjectMapper.
     */
    private ObjectMapper mapper;

    /**
     * Instantiates a new Kafka service.
     *
     * @param kafkaTemplate the kafka template
     * @param mapper        the mapper
     */
    public KafkaProducerService(final KafkaTemplate kafkaTemplate, final ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    /**
     * Send rpc to kafka.
     *
     * @param topic        the topic
     * @param kafkaPayload the kafka payload
     */
    public void sendLogsToKafka(final String topic,
                                final Map<String, Object> kafkaPayload) {
        try {
            kafkaTemplate.send(
                    topic,
                    mapper.writeValueAsString(UUID.randomUUID()),
                    mapper.writeValueAsString(kafkaPayload));
        } catch (Exception e) {
            log.error("Exception occurred while publishing to kafka topic:: "
                    + topic + " " + e);
        }
    }
}
