package org.manjunath.flink;

import org.springframework.web.servlet.tags.form.SelectTag;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;

public class Test {

    public static void main(String[] args) throws IOException {

        String path = "D:\\personal\\docs\\interview";

        /*Files.walk(Paths.get(path))
                .filter(Files :: isRegularFile)
                .map(filePath -> filePath.getFileName().toString())
                .filter(fileName -> fileName.endsWith(".txt"))
                .forEach(System.out :: println);

        System.out.println("----------------------------");
        Files.walk(Paths.get(path))
                .filter(Files :: isDirectory)
                .map(path1 -> path1.getFileName())
                .forEach(System.out :: println);*/
        //traverseFolder(new File(path));
    }

    public static void traverseFolder(final File file) {
        for (File fileEntry : file.listFiles()) {
            if (fileEntry.isDirectory()) {
                traverseFolder(fileEntry);
            } else {
                System.out.println(fileEntry.getAbsolutePath());
                System.out.println(fileEntry.getName());
            }
        }
    }
}
