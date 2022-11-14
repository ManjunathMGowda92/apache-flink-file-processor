package org.manjunath.flink.service;

public interface FlinkMessageInfoService {

    void readFromKafka();

    void readFromFileSource(String filePath);
}
