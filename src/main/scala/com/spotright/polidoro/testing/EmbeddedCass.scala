/*
 * ******************************************************************************
 *    Copyright 2012-2013 SpotRight
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 * ******************************************************************************
 */

package com.spotright.polidoro.testing

import scala.collection.immutable
import scala.util.matching.{Regex, UnanchoredRegex}

import java.io.File
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import org.apache.cassandra.service.CassandraDaemon

import org.apache.commons.io.{FileUtils => ApacheFileUtils}

import org.slf4j.LoggerFactory

object EmbeddedCass {

  implicit
  class UnanchoredWrapper[A <% immutable.StringLike[_]](str: A) {
    def qr(): UnanchoredRegex = new Regex(str.toString).unanchored
    def qr(groups: String*): UnanchoredRegex = new Regex(str.toString, groups: _*).unanchored
  }

  object Gate extends Enumeration {
    type Step = Value
    val Seeking, Seen, Expect, Done = Value
  }
}

class EmbeddedCass(
                      basedir: String = "target/cassandra.home",
                      subdir: String = "default",
                      yamlFile: String = "/cassandra.yaml",
                      dataSource: String = ""
                      )
  extends EmbeddedCassFileUtils {

  private val log = LoggerFactory.getLogger(this.getClass)

  private final var cassandraDaemon: CassandraDaemon = _
  private var _port: Int = _
  private final val executor: ExecutorService = Executors.newSingleThreadExecutor()

  def port: Option[Int] = Option(_port)

  /**
   * Set up embedded cassandra and spawn it in a new thread.
   *
   * @throws TTransportException
   * @throws IOException
   * @throws InterruptedException
   */
  def start() {
    if (cassandraDaemon != null) return

    val runtimeDir = basedir + { if (subdir.isEmpty) subdir else "/" + subdir}

    // delete runtime dir first
    rmdir(runtimeDir)
    // make a tmp dir and copy cassandra.yaml and log4j.properties to it
    copy("/log4j.properties", runtimeDir)

    // Modify yamlFile to reflect local reality
    val rtConfname = "cassandra.yaml"
    _port = copyAndModifyYamlFile(yamlFile, runtimeDir, rtConfname)

    log.info("Configuring EmbeddedCassandra on port " + port.get + " from " + runtimeDir)

    // yamlFile may be user defined; make sure to grab the copy directly from runtimeDir
    System.setProperty("cassandra.config", "file:" + runtimeDir + "/" + rtConfname)
    System.setProperty("log4j.configuration", "file:" + runtimeDir + "/log4j.properties")
    System.setProperty("cassandra-foreground","true")

    cleanupAndLeaveDirs()

    // If there is a dataSource copy the contents to runtime/data/
    if (!dataSource.isEmpty) {
      ApacheFileUtils.copyDirectory(new File(dataSource), new File(runtimeDir, "data"))
    }

    //loadYamlTables()
    log.info("Starting executor")

    executor.execute(new CassandraRunner())

    log.info("Started executor")
    try {
      TimeUnit.SECONDS.sleep(3)
      log.info("Done sleeping")
    }
    catch {
      case e: InterruptedException => throw new AssertionError(e)
    }
  }

  def shutdown() {
    if (cassandraDaemon != null) {
      cassandraDaemon.deactivate()
      cassandraDaemon == null

      executor.shutdown()
      executor.shutdownNow()
      log.info("Teardown complete")
    }
  }

  class CassandraRunner extends Runnable {

   override
   def run() {
      cassandraDaemon = new CassandraDaemon()
      cassandraDaemon.activate()
    }
  }
}
