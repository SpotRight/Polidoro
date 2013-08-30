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
package testing.keyspaces

import scala.collection.JavaConverters._

import com.netflix.astyanax.ddl.{KeyspaceDefinition, ColumnFamilyDefinition}
import com.netflix.astyanax.Cluster

trait KeyspaceLoader extends util.LogHelper {

  val keyspaceName: String

  protected val cdefs = List.newBuilder[Cluster => ColumnFamilyDefinition]

  def ksdef(cluster: Cluster): KeyspaceDefinition = {
    val ks = cluster.makeKeyspaceDefinition()
      .setName(keyspaceName)
      .setStrategyClass(classOf[org.apache.cassandra.locator.SimpleStrategy].getName)
      .setStrategyOptions(
        Map(
          "replication_factor" -> "1"
        ).asJava
      )

    cdefs.result.foreach {
      cdef =>
       ks.addColumnFamily(cdef(cluster))
    }

    ks
  }

  def load(cluster: Cluster) {
    if (cluster.describeKeyspace(keyspaceName) != null) {
      logger.warn("Test keyspace \"{}\" found. Will not load.", keyspaceName)
    }
    else {
      logger.info("Loading test keyspace \"{}\"", keyspaceName)
      val ksDef = ksdef(cluster)
      cluster.addKeyspace(ksDef)
    }
  }
}
