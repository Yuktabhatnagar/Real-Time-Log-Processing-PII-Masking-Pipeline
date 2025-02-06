package com.example.kafkatask.controller;

import com.example.kafkatask.business.LogServiceProcessing;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
    public void sendMessage(@RequestParam Map<String, Object> logs) {
        logService.SendLogs(TOPIC_1, logs);
    }
}

