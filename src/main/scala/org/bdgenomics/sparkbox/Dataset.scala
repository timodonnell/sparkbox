package org.bdgenomics.sparkbox

import au.com.bytecode.opencsv.CSVParser
import org.kohsuke.args4j.{ Option => Opt }

import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * An RDD of LabeledPoint instances where each feature has a name.
 *
 * @param featureNames a name for each column in the matrix
 * @param data the matrix
 */
case class Dataset(featureNames: Seq[String],
                   data: RDD[LabeledPoint]) {

  /** Map from column name to column index. */
  lazy val featureIndices = featureNames.zipWithIndex.toMap

  def transformData(function: RDD[LabeledPoint] => RDD[LabeledPoint]): Dataset = {
    Dataset(featureNames, function(data))
  }

  /** Return a new instance with only the given features. */
  def project(newFeatures: Seq[String]): Dataset = {
    projectByIndices(newFeatures.map(featureIndices))
  }

  /** Return a new instance with only the given features (specified by their index). */
  def projectByIndices(newFeatureIndices: Seq[Int]): Dataset = {
    val newFeatureNames = newFeatureIndices.map(featureNames)
    if (newFeatureNames == featureNames) {
      this
    } else {
      val newData = data.map(point => {
        LabeledPoint(point.label, new linalg.DenseVector(newFeatureIndices.map(point.features.apply).toArray))
      })
      Dataset(newFeatureNames, newData)
    }
  }

  /** Return a new instance including only those features that have at least two distinct values. */
  def withoutConstantFeatures(): Dataset = {
    // For each feature we keep Some(value) if we've only seen one value so far, otherwise None
    val featureIndicesToKeep = data.map(_.features.toArray.map(Some(_): Option[Double])).reduce((array1, array2) => {
      array1.zip(array2).map({
        case (Some(value1), Some(value2)) if (value1 == value2) => Some(value1)
        case _ => None
      })
    }).zipWithIndex.filter(_._1.isEmpty).map(_._2).toSeq
    projectByIndices(featureIndicesToKeep)
  }
}
object Dataset extends Logging {
  trait ReadArguments extends Common.Arguments.Base {
    @Opt(name = "--input", metaVar = "X", usage = "Input data in csv", required = true)
    var input: String = ""

    @Opt(name = "--max-features", metaVar = "X", usage = "Use only the first X features")
    var maxFeatures: Int = 0

    @Opt(name = "--ignore", metaVar = "X", usage = "Comma separated list of field names to ignore")
    var ignore: String = ""

    @Opt(name = "--target", metaVar = "X", usage = "Target column name if training")
    var target: String = "Y"
  }

  /**
   * Load from CSV file.
   *
   * @param sc spark context
   * @param filePath path to csv file
   * @param labelColumnName column name of the label column. If None, all points will have 0.0 as their label.
   * @param ignoreColumns names of columns to exclude from the result
   * @param maxColumns use only this many feature columns (i.e. exclude all but the first N)
   * @return Dataset instance
   */
  def readFromCSV(sc: SparkContext,
                  filePath: String,
                  labelColumnName: Option[String] = None,
                  ignoreColumns: Set[String] = Set.empty,
                  maxColumns: Option[Int] = None): Dataset = {
    // Is there a more efficient way to do this?
    val rdd = sc.textFile(filePath).mapPartitions(lines => {
      val parser = new CSVParser(',', '"', '\\', false, true)
      lines.map(parser.parseLine(_).map(_.trim).toIndexedSeq)
    })
    val allColumnNames: Seq[String] = rdd.first()
    if (allColumnNames.distinct.size < allColumnNames.size) {
      log.warn("Duplicate column names: %s".format(
        allColumnNames.groupBy(x => x: String).filter(_._2.length > 1).map(_._1).mkString(", ")))
    }
    val columnIndexMap = allColumnNames.zipWithIndex.toMap
    val labelColumnIndex: Int = labelColumnName match {
      case Some(name) => columnIndexMap(name)
      case None       => -1
    }
    val featureColumnNames = {
      val filteredColumns = allColumnNames.filter(name =>
        !ignoreColumns.contains(name) && !labelColumnName.exists(name == _))
      maxColumns match {
        case Some(num) => filteredColumns.take(num)
        case None      => filteredColumns
      }
    }
    val featureColumnIndices = featureColumnNames.map(columnIndexMap)

    val points = rdd.filter(_ != allColumnNames).map(row => {
      val label: Double = if (labelColumnIndex == -1) 0.0 else asNumerical(row(labelColumnIndex))
      val features = featureColumnIndices.map(i => asNumerical(row(i)))
      LabeledPoint(label, new linalg.DenseVector(features.toArray))
    })
    Dataset(featureColumnNames, points)
  }

  def fromArguments(sc: SparkContext, args: ReadArguments): Dataset = {
    val columnsToIgnore = args.ignore.split(",").map(_.trim).filter(_.nonEmpty).toSet
    val maxFeaturesOption = if (args.maxFeatures == 0) None else Some(args.maxFeatures)
    val target = if (args.target.isEmpty) None else Some(args.target)
    val points = Dataset.readFromCSV(sc, args.input, target, columnsToIgnore, maxFeaturesOption)
    Common.progress("Loaded %,d x %,d matrix into %,d partitions.".format(
      points.data.count(),
      points.featureNames.size,
      points.data.partitions.length))
    points
  }

  /**
   * Cast a string to a double.
   */
  def asNumerical(value: String): Double = {
    try {
      value.toDouble
    } catch {
      case e: NumberFormatException => if (value.toBoolean) 1.0 else 0.0
    }
  }
}
