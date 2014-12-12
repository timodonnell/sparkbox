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
import org.bdgenomics.sparkbox.{ LinearModelWithFeatureNames, Dataset, SparkCommand, Common }
import org.kohsuke.args4j.Option

/**
 * Train a generalized linear model
 *
 */
object TrainLinear {
  protected class ArgumentsWithoutTarget extends Common.Arguments.Base with Dataset.ReadArguments {
    @Option(name = "--out", metaVar = "X", usage = "Write resulting model to X")
    var out: String = ""

    @Option(name = "--kind", metaVar = "X", usage = "Link function to use, one of: logistic, linear")
    var kind: String = "logistic"

    @Option(name = "--num-iterations", metaVar = "X", usage = "Iterations to train model")
    var numIterations: Int = 200

    @Option(name = "--step-size", metaVar = "X", usage = "Advanced")
    var stepSize: Double = 1.0

    @Option(name = "--reg-param", metaVar = "X", usage = "Regularization parameter")
    var regParam: Double = 1.0
  }

  object Command extends SparkCommand[ArgumentsWithoutTarget] {
    override val name = "train-linear"
    override val description = "fit a logistic regression model"

    override def run(args: ArgumentsWithoutTarget, sc: SparkContext): Unit = {
      val points = Dataset.fromArguments(sc, args)

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
      val model = LinearModelWithFeatureNames(points.featureNames, algorithm.run(points.data.cache()))
      Common.progress("Done training model.")
      println(model)

      if (args.out.nonEmpty) {
        Common.progress("Writing model.")
        model.write(args.out)
        Common.progress("Wrote: %s".format(args.out))
      }
    }
  }
}
