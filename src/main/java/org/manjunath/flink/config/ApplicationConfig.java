package org.manjunath.flink.config;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfig {

    @Bean
    public StreamExecutionEnvironment executionEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
