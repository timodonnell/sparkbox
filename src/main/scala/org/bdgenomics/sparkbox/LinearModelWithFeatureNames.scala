package org.bdgenomics.sparkbox

import org.apache.avro.file.{ DataFileWriter, DataFileReader }
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.{ SpecificDatumWriter, SpecificDatumReader }
import org.apache.hadoop.fs.{ FileContext, AvroFSInput, Path, FileSystem }
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import scala.collection.JavaConversions._

import scala.collection.JavaConversions

case class LinearModelWithFeatureNames(featureNames: Seq[String], model: GeneralizedLinearModel) {
  assume(featureNames.length == model.weights.size)

  def write(filename: String) = {
    val record = toAvro
    val filesystem = FileSystem.get(Common.hadoopConfiguration)
    val datumWriter = new SpecificDatumWriter[avro.LinearModel]()
    val stream = filesystem.create(new Path(filename))
    val writer = new DataFileWriter[avro.LinearModel](datumWriter)

    /* TODO: support JSON encoding. */
    /*
    val encoder = if (filename.endsWith(".json")) {
      EncoderFactory.get.jsonEncoder(record.getSchema, stream, true)
    } else if (filename.endsWith(".avro")) {
      EncoderFactory.get.binaryEncoder(stream, null)
    } else {
      throw new IllegalArgumentException("Unrecognized file extension for filename: %s".format(filename))
    }
    */
    writer.create(record.getSchema, stream)
    writer.append(record)
    writer.close()
    stream.close()
  }

  def toAvro(): avro.LinearModel = {
    val builder = avro.LinearModel.newBuilder
    builder.setProvenance(Provenance.get())
    builder.setIntercept(model.intercept)
    val weights = featureNames.zip(model.weights.toArray).map(pair => new avro.Weight(pair._1, pair._2))
    builder.setWeights(JavaConversions.seqAsJavaList(weights))
    builder.build()
  }

  override def toString: String = {
    val builder = StringBuilder.newBuilder

    builder ++= "MODEL WEIGHTS:\n"
    (0 until featureNames.size).sortBy(i => math.abs(model.weights(i)) * -1).filter(model.weights(_) != 0.0).foreach(i => {
      builder ++= " [%10d] %20s = %.15f\n".format(i, featureNames(i), model.weights(i))
    })
    val numZeroFeatures = model.weights.toArray.count(_ == 0.0)
    builder ++= "Remaining %,d / %,d = %,f%% features have 0 weight.\n".format(
      numZeroFeatures, model.weights.size, numZeroFeatures * 100.0 / model.weights.size)
    builder.result
  }
}
object LinearModelWithFeatureNames {
  def readFromFile(filename: String): LinearModelWithFeatureNames = {
    val path = new Path(filename)
    val input = new AvroFSInput(FileContext.getFileContext(Common.hadoopConfiguration), path)
    val reader = new SpecificDatumReader[avro.LinearModel]()
    reader.setSchema(avro.LinearModel.getClassSchema)
    val fileReader = new DataFileReader(input, reader)
    val result = fileReader.next()
    fileReader.close()
    fromAvro(result)
  }

  def fromAvro(record: avro.LinearModel) = {
    val featureNames = record.getWeights.map(_.getName).map(_.toString)
    val weights = new DenseVector(record.getWeights.map(_.getValue.toDouble).toArray)
    LinearModelWithFeatureNames(
      featureNames,
      new LogisticRegressionModel(weights, record.getIntercept))
  }
}
