package com.linkedin.spark.datasources.tfrecord

import java.io.{DataInputStream, DataOutputStream, IOException, ObjectInputStream, ObjectOutputStream, Serializable}

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import org.tensorflow.example.{Example, SequenceExample}
import org.tensorflow.hadoop.io.TFRecordFileInputFormat

import scala.util.control.NonFatal

class DefaultSource extends FileFormat with DataSourceRegister {
  override val shortName: String = "tfrecord"

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val recordType = options.getOrElse("recordType", "Example")
    files.collectFirst {
      case f if hasSchema(sparkSession, f, recordType) => getSchemaFromFile(sparkSession, f, recordType)
    }
  }

  /**
   * Get schema from a file
   * @param sparkSession A spark session.
   * @param file The file where schema to be extracted.
   * @param recordType Example or SequenceExample
   * @return the extracted schema (a StructType).
   */
  private def getSchemaFromFile(
      sparkSession: SparkSession,
      file: FileStatus,
      recordType: String): StructType = {
    val rdd = sparkSession.sparkContext.newAPIHadoopFile(file.getPath.toString,
      classOf[TFRecordFileInputFormat], classOf[BytesWritable], classOf[NullWritable])
    recordType match {
      case "ByteArray" =>
        TensorFlowInferSchema.getSchemaForByteArray()
      case "Example" =>
        val exampleRdd = rdd.map{case (bytesWritable, nullWritable) =>
          Example.parseFrom(bytesWritable.getBytes)
        }
        TensorFlowInferSchema(exampleRdd)
      case "SequenceExample" =>
        val sequenceExampleRdd = rdd.map{case (bytesWritable, nullWritable) =>
          SequenceExample.parseFrom(bytesWritable.getBytes)
        }
        TensorFlowInferSchema(sequenceExampleRdd)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported recordType ${recordType}: recordType can be ByteArray, Example or SequenceExample")
    }
  }

  /**
   * Check if a non-empty schema can be extracted from a file.
   * The schema is empty if one of the following is true:
   *   1. The file size is zero.
   *   2. The file size is non-zero, but the schema is empty (e.g. empty .gz file)
   * @param sparkSession A spark session.
   * @param file The file where schema to be extracted.
   * @param recordType Example or SequenceExample
   * @return true if schema is non-empty.
   */
  private def hasSchema(
      sparkSession: SparkSession,
      file: FileStatus,
      recordType: String): Boolean = {
    (file.getLen > 0) && (getSchemaFromFile(sparkSession, file, recordType).length > 0)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val codec = options.getOrElse("codec", "")
    if (!codec.isEmpty) {
      conf.set("mapreduce.output.fileoutputformat.compress", "true")
      conf.set("mapreduce.output.fileoutputformat.compress.type", CompressionType.BLOCK.toString)
      conf.set("mapreduce.output.fileoutputformat.compress.codec", codec)
      conf.set("mapreduce.map.output.compress", "true")
      conf.set("mapreduce.map.output.compress.codec", codec)
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new TFRecordOutputWriter(path, options, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".tfrecord" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      TFRecordFileReader.readFile(
        broadcastedHadoopConf.value.value,
        options,
        file,
        requiredSchema)
    }
  }

  override def toString: String = "TFRECORD"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[DefaultSource]
}

private [tfrecord] class SerializableConfiguration(@transient var value: Configuration)
  extends Serializable with KryoSerializable {
  @transient private[tfrecord] lazy val log = LoggerFactory.getLogger(getClass)

  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }

  private def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        log.error("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        log.error("Exception encountered", e)
        throw new IOException(e)
    }
  }

  def write(kryo: Kryo, out: Output): Unit = {
    val dos = new DataOutputStream(out)
    value.write(dos)
    dos.flush()
  }

  def read(kryo: Kryo, in: Input): Unit = {
    value = new Configuration(false)
    value.readFields(new DataInputStream(in))
  }
}
