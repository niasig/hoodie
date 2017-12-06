/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.table.HoodieTable;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public abstract class HoodieIOHandle<T extends HoodieRecordPayload> {
    private static Logger logger = LogManager.getLogger(HoodieIOHandle.class);
    protected final String commitTime;
    protected final HoodieWriteConfig config;
    protected final FileSystem fs;
    protected final HoodieTable<T> hoodieTable;
    protected HoodieTimeline hoodieTimeline;
    protected TableFileSystemView.ReadOptimizedView fileSystemView;
    protected final Schema schema;

    public HoodieIOHandle(HoodieWriteConfig config, String commitTime,
                          HoodieTable<T> hoodieTable) {
        this.commitTime = commitTime;
        this.config = config;
        this.fs = FSUtils.getFs(config.getBasePath());
        this.hoodieTable = hoodieTable;
        this.hoodieTimeline = hoodieTable.getCompletedCommitTimeline();
        this.fileSystemView = hoodieTable.getROFileSystemView();
        this.schema =
            HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
    }

    public Path makeNewPath(String partitionPath, int taskPartitionId, String fileName) {
        Path path = new Path(config.getBasePath(), partitionPath);
        try {
            fs.mkdirs(path); // create a new partition as needed.
        } catch (IOException e) {
            throw new HoodieIOException("Failed to make dir " + path, e);
        }

        return new Path(path.toString(),
            FSUtils.makeDataFileName(commitTime, taskPartitionId, fileName));
    }

    /**
     * Deletes any new tmp files written during the current commit, into the partition
     */
    public static void cleanupTmpFilesFromCurrentCommit(HoodieWriteConfig config,
                                                        String commitTime,
                                                        String partitionPath,
                                                        int taskPartitionId) {
        FileSystem fs = FSUtils.getFs(config.getBasePath());
        try {
            FileStatus[] prevFailedFiles = fs.globStatus(new Path(String
                .format("%s/%s/%s", config.getBasePath(), partitionPath,
                    FSUtils.maskWithoutFileId(commitTime, taskPartitionId))));
            if (prevFailedFiles != null) {
                logger.info("Deleting " + prevFailedFiles.length
                    + " files generated by previous failed attempts.");
                for (FileStatus status : prevFailedFiles) {
                    fs.delete(status.getPath(), false);
                    S3Utils.emrfsDelete(status.getPath());
                }
            }
            S3Utils.sync(new Path(config.getBasePath(), partitionPath));
        } catch (IOException e) {
            throw new HoodieIOException("Failed to cleanup Temp files from commit " + commitTime,
                e);
        }
    }

    public Schema getSchema() {
        return schema;
    }
}
