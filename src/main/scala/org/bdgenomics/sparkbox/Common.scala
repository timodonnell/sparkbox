/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.sparkbox

import java.util.Calendar
import org.apache.spark.{ Logging, SparkConf, SparkContext }
import org.kohsuke.args4j.{ Option => Opt }

/**
 * Basic functions that most commands need, and specifications of command-line arguments that they use.
 *
 */
object Common extends Logging {
  object Arguments {
    /** Common argument(s) we always want.*/
    trait Base extends Args4jBase {
      @Opt(name = "--debug", usage = "If set, prints a higher level of debug output.")
      var debug = false
    }
  }

  /**
   * Parse spark environment variables from commandline. Copied from ADAM.
   *
   * Commandline format is -spark_env foo=1 -spark_env bar=2
   * @param envVariables The variables found on the commandline
   * @return array of (key, value) pairs parsed from the command line.
   */
  def parseEnvVariables(envVariables: Seq[String]): Seq[(String, String)] = {
    envVariables.foldLeft(Seq.empty[(String, String)]) {
      (a, kv) =>
        val kvSplit = kv.split("=")
        if (kvSplit.size != 2) {
          throw new IllegalArgumentException("Env variables should be key=value syntax, e.g. -spark_env foo=bar")
        }
        a :+ (kvSplit(0), kvSplit(1))
    }
  }

  /**
   *
   * Return a spark context.
   *
   * NOTE: Most properties are set through config file
   *
   * @param appName
   * @return
   */
  def createSparkContext(appName: Option[String] = None): SparkContext = {
    val config: SparkConf = new SparkConf()
    appName match {
      case Some(name) => config.setAppName("sparkbox: %s".format(name))
      case _          => config.setAppName("sparkbox")
    }

    if (config.getOption("spark.master").isEmpty) {
      config.setMaster("local[%d]".format(Runtime.getRuntime.availableProcessors()))
    }

    if (config.getOption("spark.serializer").isEmpty) {
      config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    }

    if (config.getOption("spark.kryo.registrator").isEmpty) {
      config.set("spark.kryo.registrator", "org.bdgenomics.sparkbox.SparkboxKryoRegistrator")
    }

    if (config.getOption("spark.kryoserializer.buffer.mb").isEmpty) {
      config.set("spark.kryoserializer.buffer.mb", "4")
    }

    if (config.getOption("spark.kryo.referenceTracking").isEmpty) {
      config.set("spark.kryo.referenceTracking", "true")
    }

    new SparkContext(config)
  }

  /** Time in milliseconds of last progress message. */
  var lastProgressTime: Long = 0

  /**
   * Print or log a progress message. For now, we just print to standard out.
   * @param message String to print or log.
   */
  def progress(message: String): Unit = {
    val current = System.currentTimeMillis
    val time = if (lastProgressTime == 0)
      Calendar.getInstance.getTime.toString
    else
      "%.2f sec. later".format((current - lastProgressTime) / 1000.0)
    println("--> [%15s]: %s".format(time, message))
    System.out.flush()
    lastProgressTime = current
  }

  /**
   * Like Scala's List.mkString method, but supports truncation.
   *
   * Return the concatenation of an iterator over strings, separated by separator, truncated to at most maxLength
   * characters. If truncation occurs, the string is terminated with ellipses.
   */
  def assembleTruncatedString(
    pieces: Iterator[String],
    maxLength: Int,
    separator: String = ",",
    ellipses: String = " [...]"): String = {
    val builder = StringBuilder.newBuilder
    var remaining: Int = maxLength
    while (pieces.hasNext && remaining > ellipses.length) {
      val string = pieces.next()
      builder.append(string)
      if (pieces.hasNext) builder.append(separator)
      remaining -= string.length + separator.length
    }
    if (pieces.hasNext) builder.append(ellipses)
    builder.result
  }
}

