package com.example.kafkatask.repository;

import com.example.kafkatask.entity.LogEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public interface LogsRepository extends JpaRepository<LogEntity, Long> {

    @Query(value = """
            SELECT CASE WHEN COUNT(*) > 0 THEN true ELSE false END
            FROM log_data
            WHERE org_id = :orgId
              AND app_id = :appId
              AND log_type = :logType
              AND system_name = :systemName
              AND service_name = :serviceName
            """, nativeQuery = true)
    Boolean existsById(@Param("orgId") String orgId,
                       @Param("appId") String appId,
                       @Param("logType") String logType,
                       @Param("systemName") String systemName,
                       @Param("serviceName") String serviceName);

    @Query(value = """
            SELECT is_masking, is_monitoring, log_level, log_target, masking, monitoring_rule,  system_name
            FROM public.logs
            WHERE org_id = :orgId
              AND app_id = :appId
              AND log_type = :logType
              AND system_name = :systemName
              AND service_name = :serviceName
            """, nativeQuery = true)
    Map<String, Object> fetchLogConfiguration(@Param("orgId") String orgId,
                                              @Param("appId") String appId,
                                              @Param("logType") String logType,
                                              @Param("systemName") String systemName,
                                              @Param("serviceName") String serviceName);



}
