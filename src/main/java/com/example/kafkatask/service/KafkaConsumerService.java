package com.example.kafkatask.service;

import com.example.kafkatask.business.LogServiceProcessing;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.example.kafkatask.constant.KafkaConstants.TOPIC_1;

@Log4j2
@Component
public class KafkaConsumerService {

    private final ObjectMapper mapper;
    private final ExecutorService executor;
    private final LogServiceProcessing logService;
    /**
     * Bean of Kafka Service.
     */
    private final KafkaProducerService kafkaProducerService;

    public KafkaConsumerService(ObjectMapper mapper, LogServiceProcessing logService, KafkaProducerService kafkaProducerService) {
        this.mapper = mapper;
        this.logService = logService;
        this.kafkaProducerService = kafkaProducerService;
        this.executor = Executors.newFixedThreadPool(200);
    }

    @KafkaListener(topics = TOPIC_1, groupId = "log-processor-group")
    public void processStreamLogs(ConsumerRecord<String, String> record) throws Exception {
        LinkedHashMap<String, Object> request = mapper.readValue(record.value(), LinkedHashMap.class);
        executor.submit(() -> {
            try {
                Boolean isProcessing = logService.processLog(request);
                if (isProcessing.equals(true)) {
                    Map<String, Object> finalData = logService.logsMasking(request);
                    logService.logProcessing(finalData);
                    if ("Y".equals(request.get("isMonitoring"))) {
                        logService.monitorLogs(finalData);
                    }
                }
            } catch (Exception e) {
                log.error("Error while processing logs message {}", e);
            }
        });
    }
}
