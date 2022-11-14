package org.manjunath.flink.customfunctions;

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
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CustomRichSinkFunction<T> extends RichSinkFunction<T> {

    private static final long serialVersionUID = -646242073439066104L;
    private transient ValueState<String> outputFilePath;
    private transient ValueState<List<T>> inputTupleList;

    private int writeBatchSize;
    private String outputDirPath;
    private String keyFieldName;
    private String valueFieldName;

    public CustomRichSinkFunction(String outputDirPath, String keyFieldName, String valueFieldName) {
        this(outputDirPath, keyFieldName, valueFieldName, 5000);
    }

    public CustomRichSinkFunction(String outputDirPath, String keyFieldName, String valueFieldName, int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
        this.outputDirPath = outputDirPath;
        this.keyFieldName = keyFieldName;
        this.valueFieldName = valueFieldName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> outputFilePathDesc = new ValueStateDescriptor<>(
                "outputFilePathDesc",
                TypeInformation.of(new TypeHint<String>() {
                })
        );

        ValueStateDescriptor<List<T>> inputTupleListDesc = new ValueStateDescriptor<>(
                "inputTupleListDesc",
                TypeInformation.of(new TypeHint<List<T>>() {
                })
        );

        outputFilePath = getRuntimeContext().getState(outputFilePathDesc);
        inputTupleList = getRuntimeContext().getState(inputTupleListDesc);
    }

    @Override
    public void close() throws Exception {
        List<T> inputTuples = inputTupleList.value() == null ? new ArrayList<T>() : inputTupleList.value();

        synchronized (inputTuples) {
            writeInputList(inputTuples);
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        List<T> inputTuples = inputTupleList.value() == null ? new ArrayList<T>() : inputTupleList.value();
        inputTuples.add(value);

        if (inputTuples.size() == writeBatchSize) {
            writeInputList(inputTuples);
            inputTuples = new ArrayList<>();
        }

        inputTupleList.update(inputTuples);
    }

    private void writeInputList(List<T> tupleList) {
        synchronized (tupleList) {
            String path = getOrInitFilePath(tupleList);
            try (PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(path, true)))) {
                for (T tuple : tupleList) {

                    // Create Filed Object Instance for the sourceMsg and also access it.
                    Field valueField = tuple.getClass().getDeclaredField(valueFieldName);
                    valueField.setAccessible(true);

                    String value = valueField.get(tuple).toString();
                    printWriter.println(value);
                }
            } catch (IOException | NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    private String getOrInitFilePath(List<T> tupleList) {
        T firstInstance = tupleList.get(0);
        String path = null;

        try {
            path = outputFilePath.value();
            if (Objects.isNull(path)) {
                Field keyField = firstInstance.getClass().getDeclaredField(keyFieldName);
                keyField.setAccessible(true);
                String keyValue = keyField.get(firstInstance).toString();
                path = Paths.get(outputDirPath, keyValue + ".txt").toString();

                setUpOutputFilePath(outputDirPath, path);

                outputFilePath.update(path);
            }
        } catch (IOException | NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        } catch (Exception e) {
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
