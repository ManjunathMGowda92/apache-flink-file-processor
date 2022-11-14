package org.manjunath.flink.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "kafka")
@Data
public class KafkaConfigProperties {

    private List<String> bootstrapServers;

    private String zookeeperServer;

    private String topic;

    private String groupId;
}
