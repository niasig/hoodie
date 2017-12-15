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

package com.uber.hoodie

import com.uber.hoodie.common.util.FSUtils
import com.uber.hoodie.hadoop.HoodieROFileCache
import com.uber.hoodie.exception.HoodieIOException
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileStatusCache, HadoopFsRelation, InMemoryFileIndex, NoopCache}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException

import scala.collection.mutable

/**
  * Created by niaschmald on 12/14/17.
  */
class HoodieInMemoryFileIndex(hoodieFileCache: HoodieROFileCache,
                              sparkSession: SparkSession,
                              rootPathsSpecified: Seq[Path],
                              parameters: Map[String, String],
                              partitionSchema: Option[StructType],
                              fileStatusCache: FileStatusCache = NoopCache) extends InMemoryFileIndex(
    sparkSession,
    rootPathsSpecified,
    parameters,
    partitionSchema,
    fileStatusCache) {

    /**
      * List leaf files of given paths. This method will submit a Spark job to do parallel
      * listing whenever there is a path having more files than the parallel partition discovery
      * discovery threshold.
      *
      * This is publicly visible for testing.
      */
    override def listLeafFiles(paths: Seq[Path]): mutable.LinkedHashSet[FileStatus] = {
        val output = mutable.LinkedHashSet[FileStatus]()
        val pathsToFetch = mutable.ArrayBuffer[Path]()
        for (path <- paths) {
            fileStatusCache.getLeafFiles(path) match {
                case Some(files) =>
                    HiveCatalogMetrics.incrementFileCacheHits(files.length)
                    output ++= files
                case None =>
                    pathsToFetch += path
            }
        }
        val discovered = hoodieListLeafFiles(pathsToFetch)
        discovered.foreach { case (path, leafFiles) =>
            HiveCatalogMetrics.incrementFilesDiscovered(leafFiles.size)
            fileStatusCache.putLeafFiles(path, leafFiles.toArray)
            output ++= leafFiles
        }
        output
    }

    private def hoodieListLeafFiles(pathsToFetch: Seq[Path]): Seq[(Path, Seq[FileStatus])] = {
        pathsToFetch.map { path =>
            val statuses = directoryIterator(path, true)
            val filteredStatuses = statuses.filter(status => status.isFile && hoodieFileCache.accept(status
                    .getPath))
            (path, filteredStatuses)
        }

    }

    private def directoryIterator(path: Path, recursive: Boolean): Seq[LocatedFileStatus] = {
        val fs = FSUtils.getFs(path, hadoopConf)
        try {
            val it = fs.listFiles(path, recursive)
            Iterator.continually {
                if (it.hasNext) Some(it.next()) else None
            }.takeWhile {
                _.isDefined
            }.flatten.toSeq
        } catch {
            case _: FileNotFoundException =>
                logWarning(s"The directory $path was not found. Was it deleted very recently?")
                Seq.empty
        }

    }
}

object HoodieInMemoryFileIndex {
    @transient lazy val logger = LoggerFactory.getLogger(classOf[HoodieInMemoryFileIndex])

    def apply(sparkSession: SparkSession,
              className: String,
              userSpecifiedSchema: StructType,
              partitionColumns: Seq[String] = Seq.empty,
              bucketSpec: Option[BucketSpec] = None,
              options: Map[String, String] = Map.empty): BaseRelation = {
        val caseInsensitiveOptions = CaseInsensitiveMap(options)
        val rootPathOpt = caseInsensitiveOptions.get("path")
        val rootPath = rootPathOpt.get
        val rootPathSeq = rootPathOpt.map { path => Seq(path) }.getOrElse(Seq.empty)
        val hadoopConf = sparkSession.sessionState.newHadoopConf()
        val fs = new Path(rootPath).getFileSystem(hadoopConf)
        val globbedPaths = rootPathSeq.flatMap { path =>
            val hdfsPath = new Path(path)
            val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
            val globPath = SparkHadoopUtil.get.globPathIfNecessary(qualified)

            if (globPath.isEmpty) {
                throw new HoodieIOException(s"Path does not exist: $qualified")
            }
            // Sufficient to check head of the globPath seq for non-glob scenario
            // Don't need to check once again if files exist in streaming mode
            if (!fs.exists(globPath.head)) {
                throw new HoodieIOException(s"Path does not exist: ${globPath.head}")
            }
            globPath
        }.toArray

        val hoodieFileCache = new HoodieROFileCache(fs, rootPath)
        val dataSchema = StructType(userSpecifiedSchema.filter(field => !hoodieFileCache.getPartitionColumns.contains(field
                .name)))
        val partitionSchema = StructType(userSpecifiedSchema.filter(field => hoodieFileCache.getPartitionColumns.contains(field
                .name)))
        val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
        val fileCatalog = new HoodieInMemoryFileIndex(hoodieFileCache,
            sparkSession, globbedPaths, options, Some(partitionSchema), fileStatusCache)

        val format = new ParquetFileFormat()
        HadoopFsRelation(
            fileCatalog,
            partitionSchema = partitionSchema,
            dataSchema = dataSchema,
            bucketSpec = bucketSpec,
            format,
            caseInsensitiveOptions)(sparkSession)
    }

}
