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

package com.spotright.polidoro

import com.netflix.astyanax.connectionpool.Host

import com.spotright.polidoro.session._
import com.spotright.polidoro.families._
import com.spotright.polidoro.testing.{ClusterLoader, DemoClusterLoader}

object SpotAsty extends TestableCassConnector with BatchOps {

  /**
   * Seeds are sought in the file named by property cassandra.seeds.file
   * defaulting to /etc/spotright/cassandra-seeds.
   *
   * File format is host[:port], one per line.  (The host may be listed by ip address.)
   * Blank lines and those with first non-whitespace char of '#' are skipped.
   * If no port is provided a default of 9160 will be used.
   *
   * If the seed file does not exist Astyanax will be directed to connect to "127.0.0.1:9160".
   */
  val productionCassSeeds = {
    val seedsFile = System.getProperty("cassandra.seeds.file", "/etc/polidoro-seeds")
    try {
      val s = io.Source.fromFile(seedsFile)
      s.getLines().map{_.trim}.filterNot{line => line.startsWith("#") || line.isEmpty}.map{new Host(_, 9160)}.toSeq
    }
    catch {
      case e: Exception =>
        List(new Host("localhost", 9160))
    }
  }

  protected var proxy: ContextContainer = new BasicAstyanaxContext("Production", productionCassSeeds)
  protected val clusterLoader: ClusterLoader = DemoClusterLoader

  // If you create a family in families be sure and create the
  // associated schema (mirroring what's in production cassandra) in testing.keyspaces
  // You will also need to modify testing.DemoClusterLoader to load the test schemas.
  object DemoFG extends DemonstrationFG(cluster)
}
