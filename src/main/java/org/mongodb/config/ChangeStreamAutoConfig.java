package org.mongodb.config;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.mongodb.model.ChangeStreamRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

@AutoConfiguration
@EnableConfigurationProperties({DiscoveryProperties.class, ChangeStreamProperties.class})
@ConditionalOnProperty(prefix = "change-stream", name = "enabled", havingValue = "true", matchIfMissing = true)
@ComponentScan("org.mongodb")
@Import(ScanRegistrar.class)
public class ChangeStreamAutoConfig {

    @Bean
    Map<String, ChangeStreamRegistry<?>> changeStreams() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    Set<String> instances() {
        return Collections.synchronizedSet(new LinkedHashSet<>());
    }
}
