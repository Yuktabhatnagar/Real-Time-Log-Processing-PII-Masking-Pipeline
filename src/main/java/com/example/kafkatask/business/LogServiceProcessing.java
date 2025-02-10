package com.example.kafkatask.business;

import com.example.kafkatask.entity.LogEntity;
import com.example.kafkatask.repository.LogsRepository;
import com.example.kafkatask.service.KafkaProducerService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.example.kafkatask.constant.KafkaConstants.TOPIC_2;
import static com.example.kafkatask.constant.LogsConstants.*;

@Slf4j
@Service
public class LogServiceProcessing {
    @Autowired
    private final LogsRepository logsRepository;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    /**
     * Bean of Kafka Service.
     */
    private final KafkaProducerService kafkaProducerService;

    @Autowired
    public LogServiceProcessing(final KafkaProducerService kafkaProducerService, LogsRepository logsRepository) {
        this.kafkaProducerService = kafkaProducerService;
        this.logsRepository = logsRepository;
    }

    public void sendLogs(String topic, Map<String, Object> logData) {
        Map<String, Object> logMap = new HashMap<>();
        logMap.put(ORG_ID, logData.get(ORG_ID));
        logMap.put(APP_ID, logData.get(APP_ID));
        logMap.put(LOG_TYPE, logData.get(LOG_TYPE));
        logMap.put(SYSTEM_NAME, logData.get(SYSTEM_NAME));
        logMap.put(SERVICE_NAME, logData.get(SERVICE_NAME));
        logMap.put(LOG_LEVEL, logData.get(LOG_LEVEL));
        logMap.put(LOG_TARGET, logData.get(LOG_TARGET));
        logMap.put(IS_MASKING, logData.get(IS_MASKING));
        logMap.put(MASKING, logData.get(MASKING));
        logMap.put(IS_MONITORING, logData.get(IS_MONITORING));
        logMap.put(MONITORING_RULES, logData.get(MONITORING_RULES));
        logMap.put(STATUS, logData.get(STATUS));
        logMap.put(LOG_DATA, logData.get(LOG_DATA));
        kafkaProducerService.sendLogsToKafka(
                topic, logMap);
    }

    public Boolean processLog(LinkedHashMap<String, Object> request) {
        return Boolean.TRUE.equals(IsExistDB(request)) && logsProcessing(request);
    }

    private boolean logsProcessing(LinkedHashMap<String, Object> request) {
        String logLevel = (String) request.get("logLevel");
        if ("ERROR".equals(logLevel)) {
            return true;
        }
        if ("NONE".equals(logLevel)) {
            return false;
        }
        log.info("Info Log");
        return false;
    }

    private Boolean IsExistDB(LinkedHashMap<String, Object> request) {
        return logsRepository.existsById(
                (String) request.get("orgId"),
                (String) request.get("appId"),
                (String) request.get("logType"),
                (String) request.get("systemName"),
                (String) request.get("serviceName")
        );
    }

    public Map<String, Object> logsMasking(LinkedHashMap<String, Object> request) {
        if (request.get("isMasking").equals(true)) {
            return maskLogs(request);
        } else {
            return request;
        }
    }

    private Map<String, Object> maskLogs(Map<String, Object> request) {
        List<String> maskRequirement = (List<String>) request.get("masking");
        Map<String, Object> logData = (Map<String, Object>) request.get(LOG_DATA);
        if (maskRequirement != null && logData != null) {
            maskRequirement.forEach(key -> {
                if (logData.containsKey(key)) {
                    Object value = logData.get(key);
                    if (value instanceof String) {
                        logData.put(key, maskValue((String) value));
                    }
                }
            });
            request.put(LOG_DATA, logData);
        }
        return request;
    }

    private String maskValue(String value) {
        int length = value.length();
        if (length <= 4) {
            return "****";
        }
        int maskLength = length - 4;
        return "*".repeat(maskLength) + value.substring(maskLength);
    }

    public void logProcessing(Map<String, Object> finalData) {
        String logTarget = (String) finalData.get("logTarget");
        if (logTarget.equals("Database")) {
            storeLogInDB(finalData);
        } else if (logTarget.equals("ES")) {
            sendLogs(TOPIC_2, finalData);
        }
    }

    private LogEntity storeLogInDB(Map<String, Object> request) {
        try {
            String orgId = (String) request.get("orgId");
            String appId = (String) request.get("appId");
            String logType = (String) request.get("logType");
            String systemName = (String) request.get("systemName");
            String serviceName = (String) request.get("serviceName");
            String logLevel = (String) request.get("logLevel");

            Map<String, Object> logData = (Map<String, Object>) request.get("logData");
            String logDataJson = objectMapper.writeValueAsString(logData);

            String logTarget= (String) request.get("logTarget");
            Boolean isMasking = (Boolean) request.get("isMasking");
            List<String> masking = (List) request.get("masking");
            Boolean isMonitoring = (Boolean) request.get("isMonitoring");
            String monitoringRule = (String) request.get("monitoringRule");
            String status = (String) request.get("status");

            // Save to DB
            LogEntity log = new LogEntity(orgId, appId, logType, systemName, serviceName, logLevel, logDataJson, logTarget, isMasking, masking, isMonitoring, monitoringRule, status);
            return logsRepository.save(log);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting logData to JSON", e);
        }
    }

    public void monitorLogs(Map<String, Object> maskedLogs) {
        //monitoring logic
    }
}
