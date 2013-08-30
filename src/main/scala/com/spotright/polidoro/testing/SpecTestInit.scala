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
package testing

import com.netflix.astyanax.connectionpool.Host

import com.spotright.polidoro.session.{CassConnectorProxy, BasicAstyanaxContext}

trait SpecTestInit extends util.LogHelper {
  self: CassConnectorProxy =>

  protected val clusterLoader: ClusterLoader

  private var embed: EmbeddedCass = _

  /**
   * Initialize and proxy EmbeddedTestCassandra in this object.
   *
   * This is a thin wrapper to initializing cascal.testing.EmbeddedTestCassandra.  Mainly this allows
   * documentation for testing to be incorporated here.
   *
   * @return An arity-0 function to shutdown the EmbeddedCass
   * @see src/test/scala/com/spotright/polidoro/AstyanaxTestPool
   */
  def specTestInit(
                    basedir: String = "target/cassandra.home",
                    subdir: String = "default",
                    yamlFile: String = "/cassandra.yaml",
                    sourceDir: String = ""): () => Unit = {
    if (embed == null) {
      embed = new EmbeddedCass(basedir, subdir, yamlFile, sourceDir)
      embed.start()
    }

    proxy = new BasicAstyanaxContext("Development", List(new Host("localhost", embed.port.get)))
    clusterLoader.load(cluster)

    () => specTestShutdown()
  }

  def specTestShutdown() {
    context.shutdown()

    if (embed != null)
      embed.shutdown()
  }

  def testPort(): Option[Int] = embed.port
}

