package com.example.kafkatask.controller;

import com.example.kafkatask.business.LogServiceProcessing;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

import static com.example.kafkatask.constant.KafkaConstants.TOPIC_1;

@RestController
@RequestMapping("/kafka")
public class LogsController {

    private final LogServiceProcessing logService;

    public LogsController(final LogServiceProcessing logService) {
        this.logService = logService;
    }

    @PostMapping("/publish")
    public void sendMessage(@RequestBody Map<String, Object> logs) {
        logService.sendLogs(TOPIC_1, logs);
    }
}

