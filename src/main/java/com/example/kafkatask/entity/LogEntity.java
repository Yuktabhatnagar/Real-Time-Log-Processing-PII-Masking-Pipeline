package com.example.kafkatask.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Entity
@Getter
@Setter
@Table(name = "logs")
public class LogEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String orgId;

    private String appId;

    private String logType;

    private String systemName;

    private String serviceName;

    private String logLevel;

    @Column(columnDefinition = "TEXT")
    private String logData; // Storing JSON as String

    private String logTarget;

    private Boolean isMasking;

    private List<String> masking;

    private Boolean isMonitoring;

    private String monitoringRule;

    private String status;

    public LogEntity(String orgId, String appId, String systemName, String logType, String logLevel, String serviceName, String logData, String logTarget, Boolean isMasking, List<String> masking, Boolean isMonitoring, String monitoringRule, String status) {
        this.orgId = orgId;
        this.appId = appId;
        this.systemName = systemName;
        this.logType = logType;
        this.logLevel = logLevel;
        this.serviceName = serviceName;
        this.logData = logData;
        this.logTarget = logTarget;
        this.isMasking = isMasking;
        this.masking = masking;
        this.isMonitoring = isMonitoring;
        this.monitoringRule = monitoringRule;
        this.status = status;
    }
}
