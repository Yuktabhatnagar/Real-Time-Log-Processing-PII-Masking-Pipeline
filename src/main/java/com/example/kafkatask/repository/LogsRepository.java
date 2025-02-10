package com.example.kafkatask.repository;

import com.example.kafkatask.entity.LogEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface LogsRepository extends JpaRepository<LogEntity, Long> {

    @Query(value = """
            SELECT CASE WHEN COUNT(*) > 0 THEN true ELSE false END
            FROM log_data
            WHERE org_id = :orgId
              AND app_id = :appId
              AND payload_type = :logType
              AND system_name = :systemName
              AND service_name = :serviceName
            """, nativeQuery = true)
    Boolean existsById(@Param("orgId") String orgId,
                       @Param("appId") String appId,
                       @Param("logType") String logType,
                       @Param("systemName") String systemName,
                       @Param("serviceName") String serviceName);

}
