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

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.Logging
import org.bdgenomics.sparkbox.commands._
import org.bdgenomics.sparkbox.Common.progress

/**
 * Sparkbox main class.
 */
object Sparkbox extends Logging {

  /**
   * Commands that are part of Sparkbox. If you add a new one, update this list.
   */
  private val commands: Seq[Command[_]] = List(
    LogisticRegression.Command
  )

  private def printUsage() = {
    println("Usage: java ... <command> [other args]\n")
    println("Available commands:")
    commands.foreach(caller => {
      println("%25s: %s".format(caller.name, caller.description))
    })
    println("\nTry java ... <command> -h for help on a particular command.")
  }

  /**
   * Entry point into Sparkbox.
   * @param args command line arguments
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      printUsage()
      System.exit(1)
    }
    val commandName = args(0)
    commands.find(_.name == commandName) match {
      case Some(command) => {
        progress("Sparkbox starting.")

        // Silence some verbose loggers.
        Logger.getLogger("akka").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty").setLevel(Level.WARN)
        Logger.getLogger("spark").setLevel(Level.WARN)
        Logger.getLogger("parquet.hadoop").setLevel(Level.FATAL)
        command.run(args.drop(1))
      }
      case None => {
        println("Unknown command: %s".format(commandName))
        printUsage()
        System.exit(1)
      }
    }
  }
}
