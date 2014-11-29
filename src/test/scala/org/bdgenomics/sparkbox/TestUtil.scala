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

import java.io.{ File, FileNotFoundException }
import java.net.ServerSocket
import java.util.UUID

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.{ IKryoRegistrar, KryoInstantiator, KryoPool }
import org.apache.commons.io.FileUtils
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest._

import scala.math._

object TestUtil extends Matchers {

  def assertAlmostEqual(a: Double, b: Double, epsilon: Double = 1e-12) {
    assert(abs(a - b) < epsilon, "|%.12f - %.12f| == %.12f >= %.12f".format(a, b, abs(a - b), epsilon))
  }

  object SparkTest extends org.scalatest.Tag("org.bdgenomics.sparkbox.SparkScalaTestFunSuite")

  object SparkLogUtil {
    /**
     * set all loggers to the given log level.  Returns a map of the value of every logger
     * @param level Log4j level
     * @param loggers Loggers to apply level to
     * @return
     */
    def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
      loggers.map {
        loggerName =>
          val logger = Logger.getLogger(loggerName)
          val prevLevel = logger.getLevel
          logger.setLevel(level)
          loggerName -> prevLevel
      }.toMap
    }

    /**
     * turn off most of spark logging.  Returns a map of the previous values so you can turn logging back to its
     * former values
     */
    def silenceSpark() = {
      setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
    }
  }

  /**
   * Delete a file or directory (recursively) if it exists.
   */
  def deleteIfExists(filename: String) = {
    val file = new File(filename)
    try {
      FileUtils.forceDelete(file)
    } catch {
      case e: FileNotFoundException => {}
    }
  }

  trait SparkFunSuite extends FunSuite with BeforeAndAfter {

    val sparkPortProperty = "spark.driver.port"

    var sc: SparkContext = _
    var maybeLevels: Option[Map[String, Level]] = None

    def createSpark(sparkName: String, silenceSpark: Boolean = true) = {
      // Silence the Spark logs if requested
      maybeLevels = if (silenceSpark) Some(SparkLogUtil.silenceSpark()) else None
      synchronized {
        // Find two unused ports
        val driverSocket = new ServerSocket(0)
        val uiSocket = new ServerSocket(0)
        val driverPort = driverSocket.getLocalPort
        val uiPort = uiSocket.getLocalPort
        driverSocket.close()
        uiSocket.close()
        val conf = new SparkConf(false)
          .setAppName("sparkbox: " + sparkName)
          .setMaster("local[4]")
          .set(sparkPortProperty, driverPort.toString)
          .set("spark.ui.port", uiPort.toString)
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "org.bdgenomics.sparkbox.SparkboxKryoRegistrator")
          .set("spark.kryoserializer.buffer.mb", "4")
          .set("spark.kryo.referenceTracking", "true")
        sc = new SparkContext(conf)
      }
    }

    def destroySpark() {
      // Stop the context
      sc.stop()
      sc = null

      // See notes at:
      // http://blog.quantifind.com/posts/spark-unit-test/
      // That post calls for clearing 'spark.master.port', but this thread
      // https://groups.google.com/forum/#!topic/spark-users/MeVzgoJXm8I
      // suggests that the property was renamed 'spark.driver.port'
      System.clearProperty(sparkPortProperty)

      maybeLevels match {
        case None =>
        case Some(levels) =>
          for ((className, level) <- levels) {
            SparkLogUtil.setLogLevels(level, List(className))
          }
      }
    }

    // As a hack to run a single unit test, you can set this to the name of a test to run only it. See the top of
    // DistributedUtilSuite for an example.
    var runOnly: String = ""

    def sparkTest(name: String, silenceSpark: Boolean = true)(body: => Unit) {
      if (runOnly.isEmpty || runOnly == name) {
        test(name, SparkTest) {
          createSpark(name, silenceSpark)
          try {
            // Run the test
            body
          } finally {
            destroySpark()
          }
        }
      }
    }
  }
}
