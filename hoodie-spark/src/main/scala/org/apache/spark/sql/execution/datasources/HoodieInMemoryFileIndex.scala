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

package org.apache.spark.sql.execution.datasources

import java.io.FileNotFoundException

import com.uber.hoodie.exception.HoodieIOException
import com.uber.hoodie.hadoop.HoodieROFileCache
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

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
        val discovered = HoodieInMemoryFileIndex.bulkListLeafFiles(
            pathsToFetch, hadoopConf, hoodieFileCache, sparkSession)
        discovered.foreach { case (path, leafFiles) =>
            HiveCatalogMetrics.incrementFilesDiscovered(leafFiles.size)
            fileStatusCache.putLeafFiles(path, leafFiles.toArray)
            output ++= leafFiles
        }
        output
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


    /** A serializable variant of HDFS's BlockLocation. */
    private case class SerializableBlockLocation(
                                                    names: Array[String],
                                                    hosts: Array[String],
                                                    offset: Long,
                                                    length: Long)

    /** A serializable variant of HDFS's FileStatus. */
    private case class SerializableFileStatus(
                                                 path: String,
                                                 length: Long,
                                                 isDir: Boolean,
                                                 blockReplication: Short,
                                                 blockSize: Long,
                                                 modificationTime: Long,
                                                 accessTime: Long,
                                                 blockLocations: Array[SerializableBlockLocation])

    /**
      * Lists a collection of paths recursively. Picks the listing strategy adaptively depending
      * on the number of paths to list.
      *
      * This may only be called on the driver.
      *
      * @return for each input path, the set of discovered files for the path
      */
    private def bulkListLeafFiles(
                                     paths: Seq[Path],
                                     hadoopConf: Configuration,
                                     filter: PathFilter,
                                     sparkSession: SparkSession): Seq[(Path, Seq[FileStatus])] = {

        // Short-circuits parallel listing when serial listing is likely to be faster.
        if (paths.size <= sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold) {
            return paths.map { path =>
                (path, listLeafFiles(path, hadoopConf, filter, Some(sparkSession)))
            }
        }

        logger.info(s"Listing leaf files and directories in parallel under: ${paths.mkString(", ")}")
        HiveCatalogMetrics.incrementParallelListingJobCount(1)

        val sparkContext = sparkSession.sparkContext
        val serializableConfiguration = new SerializableConfiguration(hadoopConf)
        val serializedPaths = paths.map(_.toString)
        val parallelPartitionDiscoveryParallelism =
            sparkSession.sessionState.conf.parallelPartitionDiscoveryParallelism

        // Set the number of parallelism to prevent following file listing from generating many tasks
        // in case of large #defaultParallelism.
        val numParallelism = Math.min(paths.size, parallelPartitionDiscoveryParallelism)

        val statusMap = sparkContext
            .parallelize(serializedPaths, numParallelism)
            .mapPartitions { pathStrings =>
                val hadoopConf = serializableConfiguration.value
                pathStrings.map(new Path(_)).toSeq.map { path =>
                    (path, listLeafFiles(path, hadoopConf, filter, None))
                }.iterator
            }.map { case (path, statuses) =>
            val serializableStatuses = statuses.map { status =>
                // Turn FileStatus into SerializableFileStatus so we can send it back to the driver
                val blockLocations = status match {
                    case f: LocatedFileStatus =>
                        f.getBlockLocations.map { loc =>
                            SerializableBlockLocation(
                                loc.getNames,
                                loc.getHosts,
                                loc.getOffset,
                                loc.getLength)
                        }

                    case _ =>
                        Array.empty[SerializableBlockLocation]
                }

                SerializableFileStatus(
                    status.getPath.toString,
                    status.getLen,
                    status.isDirectory,
                    status.getReplication,
                    status.getBlockSize,
                    status.getModificationTime,
                    status.getAccessTime,
                    blockLocations)
            }
            (path.toString, serializableStatuses)
        }.collect()

        // turn SerializableFileStatus back to Status
        statusMap.map { case (path, serializableStatuses) =>
            val statuses = serializableStatuses.map { f =>
                val blockLocations = f.blockLocations.map { loc =>
                    new BlockLocation(loc.names, loc.hosts, loc.offset, loc.length)
                }
                new LocatedFileStatus(
                    new FileStatus(
                        f.length, f.isDir, f.blockReplication, f.blockSize, f.modificationTime,
                        new Path(f.path)),
                    blockLocations)
            }
            (new Path(path), statuses)
        }
    }

    /**
      * Lists a single filesystem path recursively. If a SparkSession object is specified, this
      * function may launch Spark jobs to parallelize listing.
      *
      * If sessionOpt is None, this may be called on executors.
      *
      * @return all children of path that match the specified filter.
      */
    private def listLeafFiles(
                                 path: Path,
                                 hadoopConf: Configuration,
                                 filter: PathFilter,
                                 sessionOpt: Option[SparkSession]): Seq[FileStatus] = {
        logger.trace(s"Listing $path")
        val fs = path.getFileSystem(hadoopConf)

        // [SPARK-17599] Prevent InMemoryFileIndex from failing if path doesn't exist
        // Note that statuses only include FileStatus for the files and dirs directly under path,
        // and does not include anything else recursively.
        val statuses = try fs.listStatus(path) catch {
            case _: FileNotFoundException =>
                logger.warn(s"The directory $path was not found. Was it deleted very recently?")
                Array.empty[FileStatus]
        }

        val filteredStatuses = statuses.filterNot(status => InMemoryFileIndex.shouldFilterOut(status.getPath.getName))

        val allLeafStatuses = {
            val (dirs, topLevelFiles) = filteredStatuses.partition(_.isDirectory)
            val nestedFiles: Seq[FileStatus] = sessionOpt match {
                case Some(session) =>
                    bulkListLeafFiles(dirs.map(_.getPath), hadoopConf, filter, session).flatMap(_._2)
                case _ =>
                    dirs.flatMap(dir => listLeafFiles(dir.getPath, hadoopConf, filter, sessionOpt))
            }
            val allFiles = topLevelFiles ++ nestedFiles
            if (filter != null) allFiles.filter(f => filter.accept(f.getPath)) else allFiles
        }

        allLeafStatuses.filterNot(status => InMemoryFileIndex.shouldFilterOut(status.getPath.getName)).map {
            case f: LocatedFileStatus =>
                f

            // NOTE:
            //
            // - Although S3/S3A/S3N file system can be quite slow for remote file metadata
            //   operations, calling `getFileBlockLocations` does no harm here since these file system
            //   implementations don't actually issue RPC for this method.
            //
            // - Here we are calling `getFileBlockLocations` in a sequential manner, but it should not
            //   be a big deal since we always use to `listLeafFilesInParallel` when the number of
            //   paths exceeds threshold.
            case f =>
                // The other constructor of LocatedFileStatus will call FileStatus.getPermission(),
                // which is very slow on some file system (RawLocalFileSystem, which is launch a
                // subprocess and parse the stdout).
                val locations = fs.getFileBlockLocations(f, 0, f.getLen)
                val lfs = new LocatedFileStatus(f.getLen, f.isDirectory, f.getReplication, f.getBlockSize,
                    f.getModificationTime, 0, null, null, null, null, f.getPath, locations)
                if (f.isSymlink) {
                    lfs.setSymlink(f.getSymlink)
                }
                lfs
        }
    }


}
