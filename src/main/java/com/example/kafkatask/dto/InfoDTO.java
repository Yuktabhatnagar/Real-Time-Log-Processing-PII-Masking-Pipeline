package com.example.kafkatask.dto;

import jakarta.persistence.Id;

public class InfoDTO {

    private String orgId;
    private String appId;
    private String logType;
    private String systemName;
    private String serviceName;

    private String logLevel;
    private String logTarget;
    private Boolean isMasking;
    private String masking;
    private Boolean isMonitoring;
    private String monitoringRule;
    private String status;

    // Getters, setters, constructors...
}
