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

import com.uber.hoodie.common.util.FSUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by niaschmald on 12/2/17.
 */
public class S3Utils {
    private static final Logger logger = LoggerFactory.getLogger(S3Utils.class);

    public static void sync(Path path) {
//        try {
//            FileSystem fs = FSUtils.getFs(path);
//            if (fs.getScheme().toLowerCase().startsWith("s3")
//                    && fs.getConf().getBoolean("fs.s3.consistent", false)) {
//                logger.info("sync: {}", path);
//                Process process = Runtime.getRuntime().exec(String.format("/usr/bin/emrfs sync %s", path.toString()));
//                int ret = process.waitFor();
//                if (ret != 0) {
//                    logger.error("sync: {} failed {}", path, ret);
//                }
//            }
//        } catch (Exception e) {
//            logger.error("sync: {}", path, e);
//        }

    }
}
