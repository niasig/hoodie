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

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.util

import com.databricks.spark.avro.SparkAvroSchemaConverters
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters._

object AvroConversionUtils {

  def createRdd(df: DataFrame, structName: String, recordNamespace: String): RDD[GenericRecord] = {
    val dataType = df.schema
    df.rdd.mapPartitions { records =>
      if (records.isEmpty) Iterator.empty
      else {
        val convertor = createConverterToAvro(dataType, structName, recordNamespace)
        records.map { x => convertor(x).asInstanceOf[GenericRecord] }
      }
    }
  }

  def createConverterToAvro(structType: StructType,
                            structName: String,
                            recordNamespace: String
                           ): (Any) => Any = {
    val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
    val schema: Schema = SparkAvroSchemaConverters.convertStructToAvro(
      structType, builder, recordNamespace)
    createStructConverterToAvro(structType, structName, schema)
  }

  def createStructConverterToAvro(structType: StructType,
                            structName: String,
                                  schema: Schema
                           ): (Any) => Any = {
    val fieldConverters = structType.fields.zip(schema.getFields.asScala).map {
      case (sparkField, avroField) =>
        createFieldConverterToAvro(sparkField.dataType, sparkField.name, schema, avroField)
    }
    (item: Any) => {
      if (item == null) {
        null
      } else {
        val record = new Record(schema)
        val convertersIterator = fieldConverters.iterator
        val fieldNamesIterator = structType.fieldNames.iterator
        val rowIterator = item.asInstanceOf[Row].toSeq.iterator

        while (convertersIterator.hasNext) {
          val converter = convertersIterator.next()
          record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
        }
        record
      }
    }
  }

  def createFieldConverterToAvro(dataType: DataType,
                                 structName: String,
                                  schema: Schema,
                                 avroField: Schema.Field
                                 ): (Any) => Any = {
    val recordNamespace = schema.getNamespace
    dataType match {
      case BinaryType => (item: Any) => item match {
        case null => null
        case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
      }
      case ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | StringType | BooleanType => identity
      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      case TimestampType => (item: Any) => if (item == null) null else item.asInstanceOf[Timestamp].getTime
      case DateType => (item: Any) => if (item == null) null else item.asInstanceOf[Date].getTime
      case ArrayType(elementType, _) =>
        val elementConverter = createFieldConverterToAvro(elementType, structName, schema, avroField)
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val sourceArray = item.asInstanceOf[Seq[Any]]
            val sourceArraySize = sourceArray.size
            val targetList = new util.ArrayList[Any](sourceArraySize)
            var idx = 0
            while (idx < sourceArraySize) {
              targetList.add(elementConverter(sourceArray(idx)))
              idx += 1
            }
            targetList
          }
        }
      case MapType(StringType, valueType, _) =>
        val valueConverter = createFieldConverterToAvro(valueType, structName, schema, avroField)
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val javaMap = new util.HashMap[String, Any]()
            item.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
              javaMap.put(key, valueConverter(value))
            }
            javaMap
          }
        }
      case structType: StructType =>
        val structSchema = getRecordSchema(avroField.schema())
        createStructConverterToAvro(structType, structName, structSchema)
    }
  }

  private def getRecordSchema(schema: Schema): Schema = {
    schema.getType match {
      case Schema.Type.UNION => getRecordSchema(schema.getTypes.get(0))
      case Schema.Type.ARRAY => getRecordSchema(schema.getElementType)
      case Schema.Type.MAP => getRecordSchema(schema.getValueType)
      case Schema.Type.RECORD => schema
    }
  }
  def convertStructTypeToAvroSchema(structType: StructType,
                                    structName: String,
                                    recordNamespace: String) : Schema = {
    val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
    SparkAvroSchemaConverters.convertStructToAvro(structType, builder, recordNamespace)
  }

  def convertAvroSchemaToStructType(avroSchema: Schema) : StructType = {
    SparkAvroSchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType];
  }
}
