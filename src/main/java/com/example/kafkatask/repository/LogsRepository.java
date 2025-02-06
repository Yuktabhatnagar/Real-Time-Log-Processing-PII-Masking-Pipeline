package com.example.kafkatask.repository;

import com.example.kafkatask.entity.Logs;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LogsRepository extends JpaRepository<Logs, String> {

    public Boolean existsById(String orgId, String appId, String logType, String systemName, String serviceName);
}
