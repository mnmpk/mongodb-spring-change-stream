package org.mongodb.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Data
@ConfigurationProperties("discovery")
@Component
public class DiscoveryProperties {
    @Data
    public class Heartbeat {
        private long interval = 5000; // 5 seconds
        private long max = 10; // 10 times
        public long getMaxTimeout() {
            return interval*max;
        }
    }
    private Heartbeat heartbeat = new Heartbeat();
    private boolean enabled = true;
    private String collection = "_instances";    
}
