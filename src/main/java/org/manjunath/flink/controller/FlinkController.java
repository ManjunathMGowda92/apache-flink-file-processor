package org.manjunath.flink.controller;


import org.manjunath.flink.model.AppResponse;
import org.manjunath.flink.model.InputPropertiesRequest;
import org.manjunath.flink.service.FlinkMessageInfoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class FlinkController {

    @Autowired
    private FlinkMessageInfoService service;

    @GetMapping("/input/read-from-file")
    public ResponseEntity<?> readDataFromFile() {
        log.info("Reading data from file started ........");

        return createApiResponse(HttpStatus.OK, "Successfully read Data from file.");
    }

    @PostMapping("/input/read-from-kafka")
    public ResponseEntity<?> readFromKafka(@RequestBody InputPropertiesRequest properties) {
        try {
            service.readFromKafka();
        } catch (Exception e) {
            return createApiResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Exception: " + e.getMessage());
        }
        return createApiResponse(HttpStatus.OK, "Sucessfully read the Information..");
    }

    @PostMapping("/input/read-from-file")
    public ResponseEntity<?> readDataFromFile(@RequestBody InputPropertiesRequest properties) {
        log.info("Properties Received are : {}", properties);
        try {
            service.readFromFileSource(properties.getInputFilePath());
        } catch (Exception e) {
            return createApiResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Exception: " + e.getMessage());
        }
        return createApiResponse(HttpStatus.OK, "Sucessfully read the Information..");
    }

    private ResponseEntity<?> createApiResponse(HttpStatus status, String message) {
        return ResponseEntity.status(status)
                .body(getResponse(message, status));
    }

    private AppResponse getResponse(String message, HttpStatus status) {
        return AppResponse.builder()
                .httpStatus(status)
                .statusCode(status.value())
                .message(message)
                .build();
    }
}
