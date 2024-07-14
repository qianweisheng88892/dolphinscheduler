package org.apache.dolphinscheduler.plugin.registry.consul.tmp;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import lombok.Builder;
import lombok.Data;

/**
 * 钱维升 创建于 2024/7/13.
 */
@Data
@Builder
public class SessionRequest {
    private String lockDelay;
    private String name;
    private String node;
    private ImmutableList<String> checks;
    private String behavior;
    private String ttl;
}
