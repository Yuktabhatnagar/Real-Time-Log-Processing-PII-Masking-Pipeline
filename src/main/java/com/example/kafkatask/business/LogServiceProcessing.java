package com.example.kafkatask.business;

import com.example.kafkatask.dto.KafkaLogs;
import com.example.kafkatask.repository.LogsRepository;
import com.example.kafkatask.service.KafkaProducerService;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.example.kafkatask.constant.KafkaConstants.TOPIC_2;
import static com.example.kafkatask.constant.LogsConstants.*;

@Log4j2
@Service
public class LogServiceProcessing {
    /**
     * Bean of Kafka Service.
     */
    private final KafkaProducerService kafkaProducerService;
    private final LogsRepository logsRepository;

    public LogServiceProcessing(final KafkaProducerService kafkaProducerService, LogsRepository logsRepository) {
        this.kafkaProducerService = kafkaProducerService;
        this.logsRepository = logsRepository;
    }

    public void SendLogs(String topic, Map<String, Object> logData) {
        Map<String, Object> logMap = new HashMap<>();
        logMap.put(APP_ID, logData.get(APP_ID));
        logMap.put(SERVICE_NAME, logData.get(SERVICE_NAME));
        logMap.put(SYSTEM_NAME, logData.get(SYSTEM_NAME));
        logMap.put(STATUS, logData.get(STATUS));
        logMap.put(IS_ERROR, logData.get(IS_ERROR));
        kafkaProducerService.sendLogsToKafka(
                topic, logMap);
    }

    public Boolean processLog(LinkedHashMap<String, Object> request) {
        return IsExistDB(request) && logsProcessing(request);
    }

    private boolean logsProcessing(LinkedHashMap<String, Object> request) {
        String logLevel = (String) request.get("logLevel");
        if ("ERROR".equals(logLevel)) {
            return true;
        }
        if ("NONE".equals(logLevel)) {
            return false;
        }
        System.out.println("Info Log");
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
            return maskLogs();
        } else {
            return request;
        }
    }

    private Map<String, Object> maskLogs() {
        return null;
    }

    public void logProcessing(Map<String, Object> maskedLogs) {
        String logLevel= (String) maskedLogs.get("logLevel");
        if(logLevel.equals("DB")){
            storeLogInDB();
        } else if (logLevel.equals("ES")) {
            raiseAnotherEventKafka(maskedLogs);
        }
    }

    private void raiseAnotherEventKafka(Map<String, Object> maskedLogs) {

    }

    private void storeLogInDB() {
    }

    public void monitorLogs(Map<String, Object> maskedLogs) {
    }
}
