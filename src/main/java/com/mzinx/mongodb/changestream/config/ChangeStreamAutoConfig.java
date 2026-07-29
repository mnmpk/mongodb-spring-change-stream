package com.mzinx.mongodb.changestream.config;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.mzinx.mongodb.changestream.ChangeStreamRegistry;
import com.mzinx.mongodb.changestream.InstanceRegistry;

@AutoConfiguration
@EnableConfigurationProperties(ChangeStreamProperties.class)
@ConditionalOnProperty(prefix = "change-stream", name = "enabled", havingValue = "true", matchIfMissing = true)
@ComponentScan("com.mzinx.mongodb.changestream")
@EnableScheduling
@Import(AutoConfigurationPackageRegistrar.class)
public class ChangeStreamAutoConfig {

    /** Registry of every change stream runtime on this instance. */
    @Bean
    ChangeStreamRegistry changeStreamRegistry() {
        return new ChangeStreamRegistry();
    }

    /** Registry of the live instances, populated by the discovery module. */
    @Bean
    InstanceRegistry instanceRegistry() {
        return new InstanceRegistry();
    }
}
