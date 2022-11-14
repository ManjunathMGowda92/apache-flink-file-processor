package org.manjunath.flink.customfunctions;

import org.manjunath.flink.model.MessageInfo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MessageInfoFunction extends RichSinkFunction<MessageInfo> {

    private static final long serialVersionUID = -2335045367687517852L;

    private transient ValueState<String> outputFilePath;
    private transient ValueState<List<MessageInfo>> inputTupleList;

    private int writeNatchSize;
    private String outputDirPath;
    private String keyFieldName;
    private String valueFieldName;

    public MessageInfoFunction(String outputDirPath, String keyFieldName, String valueFieldName) {
        this(10000, outputDirPath, keyFieldName, valueFieldName);
    }

    public MessageInfoFunction(int writeNatchSize, String outputDirPath, String keyFieldName, String valueFieldName) {
        this.writeNatchSize = writeNatchSize;
        this.outputDirPath = outputDirPath;
        this.keyFieldName = keyFieldName;
        this.valueFieldName = valueFieldName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> outputFilePathDes = new ValueStateDescriptor<>(
                "outputFilePathDes",
                TypeInformation.of(new TypeHint<String>() {
                })
        );

        ValueStateDescriptor<List<MessageInfo>> inputTuplesListDes = new ValueStateDescriptor<>(
                "inputTuplesListDes",
                TypeInformation.of(new TypeHint<List<MessageInfo>>() {
                })
        );

        outputFilePath = getRuntimeContext().getState(outputFilePathDes);
        inputTupleList = getRuntimeContext().getState(inputTuplesListDes);
    }

    @Override
    public void close() throws Exception {
        if (Objects.nonNull(inputTupleList)) {
            List<MessageInfo> inputTuples = inputTupleList.value() == null ? new ArrayList<>() : inputTupleList.value();
            writeInputList(inputTuples);
        }
    }



    @Override
    public void invoke(MessageInfo value, Context context) throws Exception {
        List<MessageInfo> inputTuples = inputTupleList.value() == null ? new ArrayList<>() : inputTupleList.value();
        inputTuples.add(value);

        if (inputTuples.size() == writeNatchSize) {
            writeInputList(inputTuples);
            inputTuples = new ArrayList<>();
        }

        inputTupleList.update(inputTuples);
    }

    private void writeInputList(List<MessageInfo> inputTuples) {

        synchronized (inputTuples) {
            String path = getOrInitFilePath(inputTuples);
            try(PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(path, true)))) {
                for (MessageInfo info : inputTuples) {
                    String value = info.getSourceMsg();
                    writer.println(value);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String getOrInitFilePath(List<MessageInfo> inputTuples) {
        MessageInfo messageInfo = inputTuples.get(0);
        String path = null;
        try {
            path = outputFilePath.value();
            if (Objects.isNull(path)) {
                String keyedField = messageInfo.getKeyedField();
                path = Paths.get(outputDirPath, keyedField+".txt").toString();

                setUpOutputFilePath(outputDirPath, path);
                outputFilePath.update(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return path;
    }

    private void setUpOutputFilePath(String outputDirPath, String path) throws IOException {

        if (!Files.exists(Paths.get(outputDirPath))) {
            Files.createDirectories(Paths.get(outputDirPath));
        }

        Files.write(Paths.get(path), "".getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
