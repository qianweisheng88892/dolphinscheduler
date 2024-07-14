package org.apache.dolphinscheduler.plugin.registry.consul;

import com.orbitz.consul.option.QueryOptions;

import org.apache.dolphinscheduler.plugin.registry.consul.tmp.SessionRequest;

import java.util.List;
import java.util.Map;

/**
 * 钱维升 创建于 2024/7/13.
 */

public interface ConsulClient {
    String createSession(SessionRequest sessionRequest);
    void renewSession(String session);
    void destroySession(String session);

    boolean acquireLock(String key, String session);
    boolean releaseLock(String key,String session);
    String getValue(String key);
    List<Map<String, String>> getValues(String key);

    List<String> getKeys(String prefix);
    void deleteKey(String key);
    boolean putValue(String key, String value);
    boolean putValue(String key,String value,String session);
    void ping();



}
