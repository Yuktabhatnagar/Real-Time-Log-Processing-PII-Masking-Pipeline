package com.example.kafkatask.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "logs")
public class Logs {
    @Id
    private String orgId;
    @Id
    private String appId;
    @Id
    private String logType;
    @Id
    private String systemName;
    @Id
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
