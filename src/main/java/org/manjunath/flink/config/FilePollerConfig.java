package org.manjunath.flink.config;

import org.manjunath.flink.filters.LastModifiedFileFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.messaging.MessageChannel;

import java.io.File;

@Configuration
@EnableIntegration
public class FilePollerConfig {

    @Value("${file.integration.input-dir-path}")
    private String inputDirPath;

    @Value("${file.integration.output-dir-path}")
    private String outputDirPath;

    @Value("${file.integration.file-pattern}")
    private String filePattern;


    @Bean
    @InboundChannelAdapter(value = "fileInputChannel", poller = @Poller(fixedDelay = "2000"))
    public MessageSource<File> fileReadingMessageSource() {
        FileReadingMessageSource source = new FileReadingMessageSource();
        source.setDirectory(new File(inputDirPath));

        CompositeFileListFilter<File> filters = new CompositeFileListFilter<>();
        filters.addFilter(new SimplePatternFileListFilter(filePattern));
        filters.addFilter(new LastModifiedFileFilter());

        source.setFilter(filters);
        return source;
    }

    @Bean
    public FileToStringTransformer fileToStringTransformer() {
        // Create FileTransformer Object with attribute deleteFiles, so that it will
        // delete the files after processing.
        return Files.toStringTransformer();
    }

    @Bean
    public IntegrationFlow processFileFlow() {
        return IntegrationFlows.from("fileInputChannel")

                //.transform(fileToStringTransformer())
                //.transform(Files.toStringTransformer())
                .handle("fileProcessor", "processFile")
                .get();
    }

    @Bean
    public MessageChannel fileInputChannel() {
        return new DirectChannel();
    }
}
