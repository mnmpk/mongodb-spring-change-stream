package com.mzinx.changestreamtest;

import java.util.concurrent.Executor;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.mongodb.autoconfigure.MongoClientSettingsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.mongodb.MongoClientSettings;

/**
 * Minimal Spring Boot application for functional tests. The change stream
 * library configures itself through its auto-configuration; this class only
 * contributes the beans a host application is expected to provide:
 * <ul>
 * <li>a {@code taskExecutor} used to run change stream cursors</li>
 * <li>a {@code CodecRegistry} required by the aggregation library</li>
 * <li>a test {@code ChangeStreamListener} recording received events</li>
 * </ul>
 */
@SpringBootConfiguration
@EnableAutoConfiguration
public class TestApplication {

    @Bean
    Executor taskExecutor() {
        return new SimpleAsyncTaskExecutor("cs-func-test-");
    }

    @Bean
    CodecRegistry pojoCodecRegistry() {
        return CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
    }

    /**
     * Registers the POJO codec registry on the {@link com.mongodb.client.MongoClient}
     * so raw driver reads/writes of library POJOs (e.g. {@code PipelineTemplate})
     * can be encoded/decoded.
     */
    @Bean
    MongoClientSettingsBuilderCustomizer pojoCodecClientCustomizer(CodecRegistry pojoCodecRegistry) {
        return builder -> builder.codecRegistry(pojoCodecRegistry);
    }

    @Bean
    TestRecordingListener testChangeStreamListener() {
        return new TestRecordingListener();
    }
}
