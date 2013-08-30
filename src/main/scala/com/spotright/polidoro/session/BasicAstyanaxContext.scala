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
package session

import com.netflix.astyanax.connectionpool.{Host, NodeDiscoveryType}
import com.netflix.astyanax.connectionpool.impl._
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.{AstyanaxConfiguration, Cluster, AstyanaxContext}

/**
 * Connection context for Cass
 */
class BasicAstyanaxContext(clusterName: String, seeds: Seq[Host], astyConfig: AstyanaxConfiguration = null)
  extends ContextContainer with util.LogHelper {

  logger.debug("Instantiating ContextContainer for cluster \"{}\"", clusterName)

  lazy val context: AstyanaxContext[Cluster] = {
    logger.debug("Building AstyanaxContext for cluster \"{}\"", clusterName)
    new AstyanaxContext.Builder()
      .forCluster(clusterName)
      .withAstyanaxConfiguration(
        if (astyConfig != null) astyConfig
        else {
          new AstyanaxConfigurationImpl()
            .setCqlVersion("3.0.0")
            .setTargetCassandraVersion("1.2")
            .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
            .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
        }
      )
      .withConnectionPoolConfiguration {
        val conf = new ConnectionPoolConfigurationImpl(clusterName)
          .setMaxConns(20)
          .setSeeds(seeds.map{_.getUrl}.mkString(","))
        conf
      }
      .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
      .buildCluster(ThriftFamilyFactory.getInstance())
  }
}
