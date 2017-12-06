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
    transient private Configuration configuration;

    
    /**
     * Its quite common, to have all files from a given partition path be passed into accept(),
     * cache the check for hoodie metadata for known partition paths and the latest versions of files
     */
    private Map<String, HoodieFileCache> hoodiePathCache;
    private Map<String, HoodieFileCache> hoodieBaseCache;

    public HoodieROTablePathFilter() {
        hoodiePathCache = new HashMap<>();
        hoodieBaseCache = new HashMap<>();
        configuration = new Configuration();
    }

    /**
     * Obtain the path, two levels from provided path
     *
     * @return said path if available, null otherwise
     */
    private Path safeGetParentsParent(Path path) {
        if (path.getParent() != null && path.getParent().getParent() != null && path.getParent().getParent().getParent() != null) {
            return path.getParent().getParent().getParent();
        }
        return null;
    }

    @Override
    public boolean accept(Path path) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Checking acceptance for path " + path);
        }
        Path folder = null;
        try {
            FileSystem fs = FileSystem.get(path.toUri(), configuration);
            // Assumes path is a file
            folder = path.getParent(); // get the immediate parent.
            HoodieFileCache hoodieFileCache = hoodiePathCache.get(folder.toString());
            if (hoodieFileCache == null) {
                if (fs.isDirectory(path)) {
                    return true;
                }

                if (HoodiePartitionMetadata.hasPartitionMetadata(fs, folder)) {
                    HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(fs, folder);
                    metadata.readFromFS();
                    Path baseDir = HoodieHiveUtil.getNthParent(folder, metadata.getPartitionDepth());
                    hoodieFileCache = hoodieBaseCache.get(baseDir.toString());
                    if (hoodieFileCache == null) {
                        hoodieFileCache = new HoodieFileCache(fs, folder, baseDir);
                        hoodieBaseCache.put(baseDir.toString(), hoodieFileCache);
                    }
                    hoodiePathCache.put(folder.toString(), hoodieFileCache);

                    if (hoodieFileCache.accept(path)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(String.format("%s Hoodie path found in cache, accepting.\n", path));
                        }
                        return true;
                    }
                } else {
                    LOG.error("Ignoring path: " + folder.toString() + ". No hoodie metadata found.");
                }
            }
            return false;
        } catch (Exception e) {
            String msg = "Error checking path :" + path +", under folder: "+ folder;
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
        Map<Pair<String, String>, String> fileToTimestamp;
        String latestCommitTime;

        private HoodieFileCache(FileSystem fs, Path folder, Path baseDir) {
            fileToTimestamp = new HashMap<>();
            latestCommitTime = "0";
            HoodieTableMetaClient metaClient =
                    new HoodieTableMetaClient(fs, baseDir.toString());

            HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();
            timeline.getInstants().map(s -> {
                try {
                    return HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(s).get());
                } catch (IOException e) {
                    throw new HoodieIOException(
                            "Failed to read all commits.", e);
                }
            }).forEach(new Consumer<HoodieCommitMetadata>() {
                @Override
                public void accept(HoodieCommitMetadata hoodieCommitMetadata) {
                    for (Map.Entry<String, String> entry : hoodieCommitMetadata.getFileIdAndRelativePaths()
                                                                               .entrySet()) {
                        String fileId = entry.getKey();
                        String relativePath = entry.getValue();
                        String commitTime = FSUtils.getCommitTimeFromPath(relativePath);
                        String relativePartitionPath = FSUtils.getPartitionPath(relativePath);
                        Pair<String, String> key = Pair.of(fileId, (baseDir + "/" + relativePartitionPath));
                        String maxCommitTime = fileToTimestamp.get(key);
                        if (maxCommitTime == null
                                || HoodieTimeline.compareTimestamps(commitTime, maxCommitTime, HoodieTimeline.GREATER)) {
                            fileToTimestamp.put(key, commitTime);

                            if (HoodieTimeline.compareTimestamps(commitTime, latestCommitTime, HoodieTimeline.GREATER)) {
                                latestCommitTime = commitTime;
                            }
                        }
                    }
                }
            });
        }

        private boolean accept(Path path) {
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
        }
    }
}
