package com.example.kafkatask.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.springframework.messaging.handler.annotation.Payload;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
public class KafkaLogs {

    String moduleId;
    List<String> businessFilter;
    private String orgId;
    private String appId;
    private String requestId;
    private String serviceName;
    private String actualOrgId;
    private String actualAppId;
    private String systemName; //systemName
    private String traceId;
    private String spanId;
    private String arn;
    private Map<String, String> filters;
    private Map<String, String> headers;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    private LocalDateTime requestTimestamp;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    private LocalDateTime responseTimestamp;
    private Double requestPayLoadSize;
    private Double responsePayLoadSize;
    private long timeTaken;
    private String loginId;
    private String status;
    private String httpMethod;
    private String url;
    private String contextPath;
    private String statusCode;  //done
    private String SourceIpAddress;//done
    private String processedByIp;
    //    private PayloadType payloadType;
    private boolean isError;
    //    private AuditPayload auditPayload;
    //    private ErrorPayload errorPayload;
    private Object errorDetails;
    //    private CustomPayload customPayload;
    private Payload payload;
    private String purgeDays;
    private Timestamp purgeTimestamp;
}
