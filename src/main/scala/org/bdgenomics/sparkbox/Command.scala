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

import org.apache.spark.{ SparkContext, Logging }
import org.kohsuke.args4j.{ CmdLineException, CmdLineParser, Option }
import scala.collection.JavaConversions

/**
 * Interface for running a command from command line arguments.
 *
 * If you add a new command, you should also update the commands list in Sparkbox.scala to include it.
 *
 */
abstract class Command[T <% Args4jBase: Manifest] extends Serializable with Logging {
  /** The name of the command, as it will be specified on the command line. */
  val name: String

  /** A short description of the command, for display in the usage info on the command line. */
  val description: String

  /**
   * Run the command.
   *
   * @param args the command line arguments, with the first one chopped off. The first argument specifies which
   *             command to run, and is therefore already consumed by Sparkbox.
   */
  def run(args: Array[String]): Unit = run(Args4j[T](args))

  def run(args: T): Unit
}

abstract class SparkCommand[T <% Args4jBase: Manifest] extends Command[T] {
  override def run(args: T): Unit = {
    val sc = Common.createSparkContext(appName = Some(name))
    try {
      run(args, sc)
    } finally {
      sc.stop()
      DelayedMessages.default.print()
    }
  }

  def run(args: T, sc: SparkContext): Unit
}

class Args4jBase {
  @Option(name = "-h", aliases = Array("-help", "--help", "-?"), usage = "Print help")
  var doPrintUsage: Boolean = false
  @Option(name = "-print_metrics", usage = "Print metrics to the log on completion")
  var printMetrics: Boolean = false
}

// Copied from ADAM
object Args4j {
  val helpOptions = Array("-h", "-help", "--help", "-?")

  def apply[T <% Args4jBase: Manifest](args: Array[String], ignoreCmdLineExceptions: Boolean = false): T = {
    val args4j: T = manifest[T].runtimeClass.asInstanceOf[Class[T]].newInstance()
    val parser = new CmdLineParser(args4j)

    def displayHelp(exitCode: Int = 0) = {
      parser.printUsage(System.out)
      System.exit(exitCode)
    }

    // Work around for help processing in Args4j
    if (args.exists(helpOptions.contains(_))) {
      displayHelp()
    }

    try {
      parser.parseArgument(JavaConversions.asJavaCollection(args.toList))
      if (args4j.doPrintUsage)
        displayHelp()
    } catch {
      case e: CmdLineException =>
        if (!ignoreCmdLineExceptions) {
          println(e.getMessage)
          displayHelp(1)
        }
    }
    args4j
  }
}