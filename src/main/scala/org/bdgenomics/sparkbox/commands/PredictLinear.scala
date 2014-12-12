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

import java.io.OutputStreamWriter

import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.{ LogisticRegressionWithLBFGS, LogisticRegressionWithSGD }
import org.apache.spark.mllib.optimization.L1Updater
import org.bdgenomics.sparkbox.{ LinearModelWithFeatureNames, Dataset, SparkCommand, Common }
import org.kohsuke.args4j.Option

/**
 * Predict from a linear model.
 *
 */
object PredictLinear {
  protected class ArgumentsWithoutTarget extends Common.Arguments.Base with Dataset.ReadArguments {
    @Option(name = "--model", metaVar = "X", usage = "Model to use", required = true)
    var model: String = ""

    @Option(name = "--row-labels", metaVar = "X", usage = "Column name to use for row labels")
    var rowLabels: String = ""

    @Option(name = "--out", metaVar = "X", usage = "Write predictions to X")
    var out: String = ""

    @Option(name = "--test", usage = "Compute accuracy")
    var test: Boolean = false
  }

  object Command extends SparkCommand[ArgumentsWithoutTarget] {
    override val name = "predict-linear"
    override val description = "use a model to make predictions"

    override def run(args: ArgumentsWithoutTarget, sc: SparkContext): Unit = {

      val model = LinearModelWithFeatureNames.readFromFile(args.model)
      val points = Dataset.fromArguments(sc, args)

      val predictions = model.model.predict(points.data.map(_.features)).collect()

      if (args.out.nonEmpty) {
        Common.progress("Writing predictions.")
        val filesystem = FileSystem.get(Common.hadoopConfiguration)
        val out = filesystem.create(new Path((args.out)))
        val stream = new OutputStreamWriter(out, "UTF-8")
        stream.write("index, prediction\n")
        predictions.zipWithIndex.foreach({
          case (value, index) => {
            stream.write("%d, %f\n".format(index, value))
          }
        })
        stream.close()
        Common.progress("Wrote: %s".format(args.out))
      }
    }
  }
}
