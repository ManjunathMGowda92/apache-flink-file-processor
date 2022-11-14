package org.manjunath.flink.processor;

import org.manjunath.flink.service.FlinkMessageInfoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
@Slf4j
public class FileProcessor {

    private static final String HEADER_FILE_NAME = "file_name";
    private static final String HEADER_FILE_ORIGINAL_NAME = "file_originalFile";

    @Autowired
    private FlinkMessageInfoService service;

    /**
     * Process Method used to process the Message&lt;String&gt; type. File
     * type will be converted into String format using FileToStringTransformer
     * Object and IntegrationFlow object transform property.
     *
     * @param message
     */
    public void process(Message<String> message) {
        String fileName = (String) message.getHeaders().get(HEADER_FILE_NAME);
        // String content = message.getPayload();
        String fileFullPath = String.valueOf(message.getHeaders().get(HEADER_FILE_ORIGINAL_NAME));

        //log.info("File Processed : {}. Content :{}", fileName, content);
        try {
            service.readFromFileSource(fileFullPath);
        } catch (Exception e) {
            log.error("Exception : {}", e.getMessage());
        }

    }

    /**
     * Process Method which handles Message&lt;File&gt; Type.
     * @param file
     */
    public void processFile(Message<File> file) {
        String fileName = String.valueOf(file.getHeaders().get(HEADER_FILE_ORIGINAL_NAME));
        service.readFromFileSource(fileName);
    }
}
