package com.example.kafkatask.business;

import com.example.kafkatask.entity.LogEntity;
import com.example.kafkatask.repository.LogsRepository;
import com.example.kafkatask.service.KafkaProducerService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
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
        logMap.put(PAYLOAD_TYPE, logData.get(PAYLOAD_TYPE));
        logMap.put(LOG_TYPE, logData.get(LOG_TYPE));
        logMap.put(SYSTEM_NAME, logData.get(SYSTEM_NAME));
        logMap.put(SERVICE_NAME, logData.get(SERVICE_NAME));
        logMap.put(STATUS, logData.get(STATUS));
        logMap.put(HEADERS, logData.get(HEADERS));
        logMap.put(TRACE_ID, logData.get(TRACE_ID));
        logMap.put(LOGIN_ID, logData.get(LOGIN_ID));
        logMap.put(STATUS_CODE, logData.get(STATUS_CODE));
        logMap.put(HTTP_METHOD, logData.get(HTTP_METHOD));
        logMap.put(URL, logData.get(URL));
        logMap.put(PAYLOAD, logData.get(PAYLOAD));
        logMap.put(PURGE_DAYS, logData.get(PURGE_DAYS));
        logMap.put(MODULE_ID, logData.get(MODULE_ID));
        kafkaProducerService.sendLogsToKafka(
                topic, logMap);
    }

    public Boolean processLog(LinkedHashMap<String, Object> request, Map<String, Object> logConfigs) {
        if (logConfigs != null) {
            return logsProcessing(logConfigs);
        }
        return false;
    }

    private boolean logsProcessing(Map<String, Object> logsConfig) {
        String logLevel = (String) logsConfig.get("logLevel");
        if ("ERROR".equals(logLevel)) {
            return true;
        }
        if ("NONE".equals(logLevel)) {
            return false;
        }
        log.info("Info Log");
        return false;
    }

    public Map<String, Object> IsExistDB(LinkedHashMap<String, Object> request) {
        return logsRepository.fetchLogConfiguration(
                (String) request.get("orgId"),
                (String) request.get("appId"),
                (String) request.get("logType"),
                (String) request.get("systemName"),
                (String) request.get("serviceName")
        );
    }

    public Map<String, Object> logsMasking(LinkedHashMap<String, Object> request, Map<String, Object> logsConfig) throws IOException {
        if (logsConfig.get("isMasking").equals(true)) {
            return maskLogs(request, logsConfig);
        } else {
            return request;
        }
    }

    private Map<String, Object> maskLogs(Map<String, Object> request, Map<String, Object> logsConfig) throws IOException {
        String[] maskRequirement = (String[]) logsConfig.get(MASKING);
        Object logData = request.get(PAYLOAD);
        if (!ObjectUtils.isEmpty(maskRequirement) && !ObjectUtils.isEmpty(logData)) {
            if (logData instanceof Map) {
                maskKeysInMap((Map<String, Object>) logData, maskRequirement);
            } else if (logData instanceof String) {
                Map<String, Object> logMapData = objectMapper.readValue((String) logData, Map.class);
                maskKeysInMap(logMapData, maskRequirement);
            }
            request.put(LOG_DATA, logData);
        }
        return request;
    }

    /**
     * Recursively masks specified keys inside a nested map.
     */
    private void maskKeysInMap(Map<String, Object> data, String[] maskRequirement) {
        for (String key : maskRequirement) {
            if (data.containsKey(key)) {
                Object value = data.get(key);
                if (value instanceof String) {
                    data.put(key, maskValue((String) value));
                }
            }
        }
        // Recursively check nested maps
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                maskKeysInMap((Map<String, Object>) value, maskRequirement);
            } else if (value instanceof List) {
                maskKeysInList((List<Object>) value, maskRequirement);
            }
        }
    }

    /**
     * Recursively masks keys inside a list (handles lists of maps).
     */
    private void maskKeysInList(List<Object> list, String[] maskRequirement) {
        for (int i = 0; i < list.size(); i++) {
            Object item = list.get(i);
            if (item instanceof Map) {
                maskKeysInMap((Map<String, Object>) item, maskRequirement);
            } else if (item instanceof String) {
                list.set(i, maskValue((String) item));
            }
        }
    }

    /**
     * Mock method to mask sensitive data.
     */

    //
    private String maskValue(String value) {
        int length = value.length();
        if (length <= 4) {
            return "****";
        }
        int maskLength = length - 4;
        return "*".repeat(maskLength) + value.substring(maskLength);
    }

    public void logProcessing(Map<String, Object> finalData, Map<String, Object> logsConfig) {
        String logTarget = (String) logsConfig.get("logTarget");
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

            String logTarget = (String) request.get("logTarget");
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

    public void monitorLogs(Map<String, Object> maskedLogs, Object monitoringRules) {
        //monitoring logic
    }
}
