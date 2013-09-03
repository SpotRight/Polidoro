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

import com.netflix.astyanax.connectionpool.Host
import com.netflix.astyanax.{Cluster, AstyanaxContext}

/**
 * Connection context for Cass
 */
class BasicAstyanaxContext(clusterName: String, seeds: Seq[Host])
  extends ContextContainer with util.LogHelper {

  lazy val context: AstyanaxContext[Cluster] = new ContextContainerImpl(
    ContextContainerConfig(clusterName, seeds)
  ).context
}
