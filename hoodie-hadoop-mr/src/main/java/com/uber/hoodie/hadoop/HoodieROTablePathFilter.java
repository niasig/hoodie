/*
 * Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.uber.hoodie.hadoop;

import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.DatasetNotFoundException;
import com.uber.hoodie.exception.HoodieException;

import com.uber.hoodie.exception.HoodieIOException;
import it.unimi.dsi.fastutil.longs.Long2BooleanArrayMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Given a path is a part of
 * - Hoodie dataset = accepts ONLY the latest version of each path
 * - Non-Hoodie dataset = then always accept
 *
 * We can set this filter, on a query engine's Hadoop Config and if it respects path filters, then
 * you should be able to query both hoodie and non-hoodie datasets as you would normally do.
 *
 * hadoopConf.setClass("mapreduce.input.pathFilter.class",
 *                      com.uber.hoodie.hadoop.HoodieROTablePathFilter.class,
 *                      org.apache.hadoop.fs.PathFilter.class)
 *
 */
public class HoodieROTablePathFilter implements PathFilter, Serializable {

    public static final Log LOG = LogFactory.getLog(HoodieROTablePathFilter.class);
    private static final String SEPERATOR = "/";
    transient private Configuration configuration;

    
    /**
     * Its quite common, to have all files from a given partition path be passed into accept(),
     * cache the check for hoodie metadata for known partition paths and the latest versions of files
     */
    transient HoodieFileCache hoodieFileCache;

    public HoodieROTablePathFilter() {
        hoodieFileCache = null;
        configuration = new Configuration();
    }

    @Override
    public boolean accept(Path path) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Checking acceptance for path " + path);
        }
        try {
            if (hoodieFileCache == null) {
                //accept all directories till hoodie cache is constructed.
                FileSystem fs = FileSystem.get(path.toUri(), configuration);
                if(fs.isDirectory(path)) {
                    return true;
                }
                // assume path is a file
                Path folder = path.getParent();
                if (HoodiePartitionMetadata.hasPartitionMetadata(fs, folder)) {
                    hoodieFileCache = new HoodieFileCache(fs, folder);
                } else {
                    LOG.error("Ignoring path: " + folder.toString() + ". No hoodie metadata found.");
                    return false;
                }
            }
            return hoodieFileCache.accept(path, configuration);
        } catch (Exception e) {
            String msg = "Error checking path :" + path;
            LOG.error(msg, e);
            throw new HoodieException(msg, e);
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        configuration = new Configuration();
    }

    private static class HoodieFileCache {
        private final Map<Pair<String, String>, String> fileToTimestamp;
        private final Set<String> folders;
        private String latestCommitTime;

        private HoodieFileCache(FileSystem fs, Path folder) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Loading HoodieFileCache for " + folder);
            }
            fileToTimestamp = new HashMap<>();
            folders = new HashSet<>();
            latestCommitTime = "0";
            HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(fs, folder);
            metadata.readFromFS();
            String baseDir = HoodieHiveUtil.getNthParent(folder, metadata.getPartitionDepth()).toString();
            HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, baseDir);

            HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();
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
                        String commitTime = FSUtils.getCommitTimeFromPath(relativePath);
                        String relativePartitionPath = FSUtils.getPartitionPath(relativePath);
                        String fileFolder = addFolders(baseDir, relativePartitionPath);
                        Pair<String, String> key = Pair.of(fileId, fileFolder);
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

        private String addFolders(String baseDir, String relativeParitionPath) {
            String path = baseDir;
            folders.add(path);
            for (String relativePath : relativeParitionPath.split(SEPERATOR)) {
                path += SEPERATOR + relativePath;
                folders.add(path);
            }
            return path;
        }

        private boolean accept(Path path, Configuration conf) throws Exception {
            if (FSUtils.isDataFile(path.toString()) && !fileToTimestamp.isEmpty()) {
                String name = path.getName();
                String fileId = FSUtils.getFileId(name);
                String fileCommitTime = FSUtils.getCommitTime(name);
                String partitionPath = FSUtils.getPartitionPath(path.toString());
                Pair<String, String> key = Pair.of(fileId, partitionPath);
                String maxCommitTime = fileToTimestamp.get(key);
                if (maxCommitTime == null) {
                    return HoodieTimeline.compareTimestamps(fileCommitTime, latestCommitTime, HoodieTimeline.LESSER_OR_EQUAL);
                } else {
                    return StringUtils.equals(fileCommitTime, maxCommitTime);
                }
            } else {
                FileSystem fs = FileSystem.get(path.toUri(), conf);
                if (fs.isDirectory(path)) {
                    return folders.contains(path.toString());
                } else {
                    return false;
                }
            }
        }
    }
}
