package org.manjunath.flink.service;

import org.manjunath.flink.config.KafkaConfigProperties;
import org.manjunath.flink.customfunctions.CustomRichSinkFunction;
import org.manjunath.flink.deserializer.MessageInfoDeserializer;
import org.manjunath.flink.model.MessageInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class FlinkMessageInfoServiceImpl implements FlinkMessageInfoService {

    @Autowired
    private StreamExecutionEnvironment executionEnvironment;

    @Autowired
    private KafkaConfigProperties kafkaProperties;

    @Autowired
    private Environment env;

    @Value("${extraction.expression}")
    private String expressionPattern;

    // 1. remove all white space preceding a begin element tag:
    private static final String REGEX1 = "[\\n\\s]+(\\<[^/])";

    // 2. remove all white space following an end element tag:
    private static final String REGEX2 = "(\\</[a-zA-Z0-9-_\\.:]+\\>)[\\s]+";

    // 3. remove all white space following an empty element tag
    // (<some-element xmlns:attr1="some-value".... />):
    private static final String REGEX3 = "(/\\>)[\\s]+";

    @Value("${flink.output.directory}")
    private String outputDirectoryPath;

    @Override
    public void readFromKafka() {
        KafkaSource<MessageInfo> kafkaSource = getKafkaSource();
        CustomRichSinkFunction<MessageInfo> sinkFunction = getSinkFunction();
        final Pattern pattern = Pattern.compile(expressionPattern);

        try {
            DataStreamSource<MessageInfo> source = executionEnvironment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
            source.filter(obj -> Objects.nonNull(obj.getSourceMsg()))
                    .map(obj -> {
                        obj.setKeyedField(extractKeyField(pattern, obj.getSourceMsg()));
                        obj.setSourceMsg(formatAndSetSourceMsg(obj.getSourceMsg()));
                        return obj;
                    }).keyBy(MessageInfo::getKeyedField)
                    .addSink(sinkFunction)
                    .setParallelism(1);

            executionEnvironment.execute();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }


    @Override
    public void readFromFileSource(String filePath) {
        final Pattern pattern = Pattern.compile(expressionPattern);
        CustomRichSinkFunction<MessageInfo> sinkFunction = getSinkFunction();
        try {
            DataStream<String> source = executionEnvironment.readTextFile(filePath);
            source.map(value -> {
                        MessageInfo obj = new MessageInfo();
                        String keyedField = extractKeyField(pattern, value);
                        keyedField = Objects.isNull(keyedField) ? "DEFAULT" : keyedField;
                        obj.setKeyedField(keyedField);
                        obj.setSourceMsg(value);
                        return obj;
                    })
                    .keyBy((KeySelector<MessageInfo, String>) MessageInfo::getKeyedField)
                    .addSink(sinkFunction)
                    .setParallelism(2);

            executionEnvironment.execute();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private KafkaSource<MessageInfo> getKafkaSource() {
        return KafkaSource.<MessageInfo>builder()
                .setBootstrapServers(kafkaProperties.getBootstrapServers().stream().collect(Collectors.joining(",")))
                .setTopics(kafkaProperties.getTopic())
                .setGroupId(kafkaProperties.getGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new MessageInfoDeserializer()))
                .build();
    }

    private CustomRichSinkFunction<MessageInfo> getSinkFunction() {
        return new CustomRichSinkFunction<>(
                outputDirectoryPath,
                env.getProperty("keyed.field"),
                env.getProperty("value.field")
        );
    }

    private static String formatAndSetSourceMsg(String sourceMsg) {
        return sourceMsg.replaceAll(REGEX1, "$1")
                .replaceAll(REGEX2, "$1")
                .replaceAll(REGEX3, "$1");
    }

    private static String extractKeyField(Pattern pattern, String message) {
        Matcher matcher = pattern.matcher(message);
        String extractedValue = null;
        while (matcher.find()) {
            extractedValue = matcher.group(1);
            break;
        }
        return extractedValue;
    }

}
