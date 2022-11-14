package org.manjunath.flink.filters;

import org.springframework.integration.file.filters.AbstractFileListFilter;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class LastModifiedFileFilter extends AbstractFileListFilter<File> {
    private final Map<String, Long> lastModifiedTimeTrackMap = new HashMap<>();
    private final Object monitor = new Object();


    @Override
    public boolean accept(File file) {
        synchronized (this.monitor) {
            Long lastModifiedTime = lastModifiedTimeTrackMap.put(file.getName(), file.lastModified());

            return lastModifiedTime == null || lastModifiedTime != file.lastModified();
        }
    }
}
