/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.hadoop;

import com.google.common.collect.ImmutableList;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class filters paths based on the hoodie metadata.  This is for RO table views it will accept only committed
 * hoodie parquet files.
 */
public class HoodieROFileCache implements PathFilter, Serializable {
    public static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HoodieROFileCache.class);

    private static final String SEPERATOR = "/";

    private final Map<Pair<String, String>, String> fileToTimestamp = new HashMap<>();
    private final Set<String> folders = new HashSet<>();
    private final Set<String> partitionPaths = new HashSet<>();
    private final List<String> partitionColumns;
    private String latestCommitTime = "0";
    private final String baseDir;

    public HoodieROFileCache(FileSystem fs, String baseDir) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading HoodieROFileCache for " + baseDir);
        }
        this.baseDir = baseDir;

        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, baseDir);

        HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();
        partitionColumns = getPartitionColumns(timeline);

        timeline.getInstants().map(s -> {
            try {
                return HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(s).get());
            } catch (IOException e) {
                throw new HoodieIOException(
                        "Failed to read all commits.", e);
            }
        }).forEach(hoodieCommitMetadata -> {
            for (Map.Entry<String, String> entry : hoodieCommitMetadata.getFileIdAndRelativePaths().entrySet()) {
                String fileId = entry.getKey();
                String relativePath = entry.getValue();
                String fullPath = baseDir + SEPERATOR + relativePath;
                String commitTime = FSUtils.getCommitTimeFromPath(new Path(fullPath));
                String relativePartitionPath = FSUtils.getPartitionPath(relativePath);
                addFolders(relativePartitionPath);
                Pair<String, String> key = toKey(fullPath);
                String maxCommitTime = fileToTimestamp.putIfAbsent(key, commitTime);
                if (maxCommitTime != null && HoodieTimeline.compareTimestamps(commitTime, maxCommitTime, HoodieTimeline.GREATER)) {
                    fileToTimestamp.put(key, commitTime);
                }
                if (HoodieTimeline.compareTimestamps(commitTime, latestCommitTime, HoodieTimeline.GREATER)) {
                    latestCommitTime = commitTime;
                }
            }

        });
    }

    private void addFolders(String relativeParitionPath) {
        String path = baseDir;
        folders.add(path);
        for (String relativePath : relativeParitionPath.split(SEPERATOR)) {
            path += SEPERATOR + relativePath;
            folders.add(path);
        }
        partitionPaths.add(relativeParitionPath);
    }

    private Pair<String, String> toKey(String fullPath) {
        Path path = new Path(fullPath);
        String fileId = FSUtils.getFileId(path.getName());
        String parentPath = path.getParent().toString();
        return Pair.of(fileId, parentPath);
    }

    private List<String> getPartitionColumns(HoodieTimeline timeline) {
        return timeline.getInstants().findFirst().map(s -> {
            try {
                return HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(s).get());
            } catch (IOException e) {
                throw new HoodieIOException(
                        "Failed to read all commits.", e);
            }
        }).flatMap(hoodieCommitMetadata ->
                hoodieCommitMetadata.getFileIdAndRelativePaths().entrySet().stream().findFirst())
                .map(Map.Entry::getValue)
                .map(relativePath -> ImmutableList.copyOf(relativePath.split(SEPERATOR)))
                // Drop the filename
                .map(fileParts -> ImmutableList.copyOf(fileParts.subList(0, fileParts.size() - 1)))
                // Split the partition column name from its value
                .map(partitionParts -> partitionParts.stream().map(part -> part.split("=")[0]))
                .map(partitionStream -> partitionStream.collect(Collectors.toList()))
                .map(partitionColumns -> ImmutableList.copyOf(partitionColumns))
                .orElse(ImmutableList.of());
    }

    public boolean accept(Path path) {
        if (FSUtils.isDataFile(path.toString()) && !fileToTimestamp.isEmpty()) {
            String pathStr = path.toString();
            Pair<String, String> key = toKey(pathStr);
            String fileCommitTime = FSUtils.getCommitTimeFromPath(path);
            String maxCommitTime = fileToTimestamp.get(key);
            if (maxCommitTime == null) {
                return HoodieTimeline.compareTimestamps(fileCommitTime, latestCommitTime, HoodieTimeline.LESSER);
            } else {
                return StringUtils.equals(fileCommitTime, maxCommitTime);
            }
        } else {
            return folders.contains(path.toString());
        }
    }

    public List<String> getPartitionColumns() {
        return partitionColumns;
    }
    public Collection<String> getPartitionPaths() { return
            folders;
    }
}
