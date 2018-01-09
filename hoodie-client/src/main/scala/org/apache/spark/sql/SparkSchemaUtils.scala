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

package org.apache.spark.sql

import com.databricks.spark.avro.SparkAvroSchemaConverters
import com.uber.hoodie.config.HoodieWriteConfig
import org.apache.avro.Schema
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.schema.MessageType
import org.apache.spark.sql.execution.datasources.parquet.SparkParquetSchemaConverters
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object SparkSchemaUtils {
    @transient lazy private val logger = LoggerFactory.getLogger(SparkSchemaUtils.getClass)

    def toParquetSchema(avroSchema: Schema, config: HoodieWriteConfig): MessageType = {
        Option(config.getSparkSchema).map { sparkSchemaStr =>
            val originalSparkFields = stringToSparkSchema(sparkSchemaStr).fields.map { f => (f.name, f) }.toMap

            val sparkSchema = avroSchema.getFields.asScala.map { avroField =>
                originalSparkFields.getOrElse(avroField.name(),
                    SparkAvroSchemaConverters.toStructField(avroField.name(), avroField.schema()))
            }
            val parquetSchema = SparkParquetSchemaConverters.convert(StructType(sparkSchema))
            parquetSchema
        }.getOrElse(new AvroSchemaConverter().convert(avroSchema))
    }

    def sparkSchemaToString(schema: StructType): String = {
        schema.json
    }

    def stringToSparkSchema(schemaStr: String): StructType = {
        StructType.fromString(schemaStr)
    }


}
