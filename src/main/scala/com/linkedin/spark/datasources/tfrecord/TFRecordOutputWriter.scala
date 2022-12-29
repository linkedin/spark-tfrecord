package com.linkedin.spark.datasources.tfrecord

import java.io.DataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType
import org.tensorflow.hadoop.util.TFRecordWriter


class TFRecordOutputWriter(
    val path: String,
    options: Map[String, String],
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter{

  private val outputStream = CodecStreams.createOutputStream(context, new Path(path))
  private val dataOutputStream = new DataOutputStream(outputStream)
  private val writer = new TFRecordWriter(dataOutputStream)
  private val recordType = options.getOrElse("recordType", "Example")

  private[this] val serializer = new TFRecordSerializer(dataSchema)

  override def write(row: InternalRow): Unit = {
    val record = recordType match {
      case "ByteArray" =>
        serializer.serializeByteArray(row)
      case "Example" =>
        serializer.serializeExample(row).toByteArray
      case "SequenceExample" =>
        serializer.serializeSequenceExample(row).toByteArray
      case _ =>
        throw new IllegalArgumentException(s"Unsupported recordType ${recordType}: recordType can be Byte Array, Example or SequenceExample")
    }
    writer.write(record)
  }

  override def close(): Unit = {
    dataOutputStream.close()
    outputStream.close()
  }
}
