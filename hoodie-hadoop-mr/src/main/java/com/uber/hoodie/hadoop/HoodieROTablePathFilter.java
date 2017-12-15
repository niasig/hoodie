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

import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

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

    public static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HoodieROTablePathFilter.class);

    /**
     * Its quite common, to have all files from a given partition path be passed into accept(),
     * cache the check for hoodie metadata for known partition paths and the latest versions of files
     */
    private HoodieROFileCache hoodieFileCache;

    public HoodieROTablePathFilter() {
        LOG.info("init: {}", this, new RuntimeException());
        hoodieFileCache = null;
    }

    @Override
    public boolean accept(Path path) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("{}: Checking acceptance for path {}", this, path);
        }
        try {
            if (hoodieFileCache == null) {
                FileSystem fs = FSUtils.getFs(path);
                // assume path is a file
                Path folder = path.getParent();
                if (HoodiePartitionMetadata.hasPartitionMetadata(fs, folder)) {
                    LOG.debug("{}: new hoodieFileCache", this);
                    HoodiePartitionMetadata metadata = new HoodiePartitionMetadata(fs, folder);
                    metadata.readFromFS();
                    String baseDir = HoodieHiveUtil.getNthParent(folder, metadata.getPartitionDepth()).toString();
                    hoodieFileCache = new HoodieROFileCache(fs, baseDir);
                } else {
                    //accept all directories till hoodie cache is constructed.
                    if(fs.isDirectory(path)) {
                        return true;
                    }
                    LOG.error("Ignoring path: " + folder.toString() + ". No hoodie metadata found.");
                    return false;
                }
            }
            return hoodieFileCache.accept(path);
        } catch (Exception e) {
            String msg = "Error checking path :" + path;
            LOG.error(msg, e);
            throw new HoodieException(msg, e);
        }
    }

}
