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

import com.netflix.astyanax.{Cluster, AstyanaxContext}
import com.spotright.polidoro.testing.SpecTestInit

trait ContextContainer {

  def context: AstyanaxContext[Cluster]
}

trait CassConnector extends util.LogHelper {
  self: ContextContainer =>

  lazy val cluster: Cluster = {
    logger.debug("Starting context for cluster \"{}\"", context.getClusterName)

    // Show how we got here.  Useful for debugging issues where you want to be
    // in Development cluster via .specTestInit but something evaluated us already.
    val e = new RuntimeException("-stacktrace-")
    logger.debug("-- Stacktrace of code path to context start --\n{}",
      e.getStackTrace.takeWhile{_.toString.startsWith("com")}.mkString("\n"))

    context.start()
    context.getClient
  }
}

trait CassConnectorProxy extends ContextContainer with CassConnector {

  protected def proxy: ContextContainer
  protected def proxy_=(cc: ContextContainer)

  def context: AstyanaxContext[Cluster] = proxy.context
}

trait TestableCassConnector extends CassConnectorProxy with SpecTestInit
