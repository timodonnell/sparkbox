/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.sparkbox.commands

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.{ LogisticRegressionWithLBFGS, LogisticRegressionWithSGD }
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.{ LassoWithSGD, LabeledPoint }
import org.bdgenomics.sparkbox.{ LabeledDataset, SparkCommand, Common }
import org.kohsuke.args4j.Option

/**
 * Simple feature selector.
 *
 */
object LogisticRegression {

  protected class Arguments extends Common.Arguments.Base {
    @Option(name = "--input", metaVar = "X", usage = "Input data in csv", required = true)
    var input: String = ""

    @Option(name = "--out", metaVar = "X", usage = "Result file")
    var out: String = ""

    @Option(name = "--target", metaVar = "X", usage = "Target column name")
    var target: String = "Y"

    @Option(name = "--max-features", metaVar = "X", usage = "Use only the first X features")
    var maxFeatures: Int = 0

    @Option(name = "--ignore", metaVar = "X", usage = "Comma separated list of field names to ignore")
    var ignore: String = ""

    @Option(name = "--num-iterations", metaVar = "X", usage = "Iterations to train model")
    var numIterations: Int = 200

    @Option(name = "--step-size", metaVar = "X", usage = "Advanced")
    var stepSize: Double = 1.0

    @Option(name = "--reg-param", metaVar = "X", usage = "Advanced")
    var regParam: Double = 1.0
  }

  object Command extends SparkCommand[Arguments] {

    override val name = "logistic-regression"
    override val description = "fit a logistic regression model"

    override def run(args: Arguments, sc: SparkContext): Unit = {

      val columnsToIgnore = args.ignore.split(",").map(_.trim).filter(_.nonEmpty).toSet
      val maxFeaturesOption = if (args.maxFeatures == 0) None else Some(args.maxFeatures)
      val rawPoints = LabeledDataset.readFromCSV(sc,
        args.input,
        Some(args.target),
        columnsToIgnore,
        maxFeaturesOption)

      val points = rawPoints.withoutConstantFeatures
      val numRemovedFeatures = rawPoints.featureNames.size - points.featureNames.size
      if (numRemovedFeatures > 0) {
        Common.progress("Dropped %,d constant features.".format(numRemovedFeatures))
      }

      Common.progress("Loaded %,d x %,d matrix into %,d partitions.".format(
        points.data.count(),
        points.featureNames.size,
        points.data.partitions.length))

      val labelCounts = points.data.map(_.label).countByValue()
      Common.progress("Points with label=1: %,d / %,d = %f%%".format(
        labelCounts(1),
        labelCounts.values.sum,
        labelCounts(1) * 100.0 / labelCounts.values.sum))

      val algorithm = new LogisticRegressionWithLBFGS()
      algorithm.optimizer
        .setNumIterations(args.numIterations)
        .setUpdater(new L1Updater())
        .setRegParam(args.regParam)

      Common.progress("Training model.")
      val model = algorithm.run(points.data.cache())
      Common.progress("Done training model.")

      println("MODEL WEIGHTS: ")
      (0 until points.featureNames.size).sortBy(i => math.abs(model.weights(i)) * -1).filter(i => model.weights(i) != 0.0).foreach(i => {
        println(" [%10d] %20s = %.15f".format(i, points.featureNames(i), model.weights(i)))
      })
      val numZeroFeatures = model.weights.toArray.count(_ == 0.0)
      println("Remaining %,d / %,d = %,f%% features have 0 weight.".format(
        numZeroFeatures, model.weights.size, numZeroFeatures * 100.0 / model.weights.size))
    }
  }
}
