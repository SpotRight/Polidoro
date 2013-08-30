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
package families

import com.spotright.polidoro.model._
import com.netflix.astyanax.Cluster

class DemonstrationFG(cluster: Cluster) {

  val ksDemo = cluster.getKeyspace("Demonstration").ensuring(_ != null, "null keyspace Demonstration$")

  val cfUsers = ColFam(ksDemo, "Users")    // keys=UTF8Type, names=UTF8Type

  val kcfCities = ColFam(ksDemo, "Cities", hasCompositeKey = true)    // keys=(UTF8Type, UTF8Type), names=UTF8Type
  val scfClusters = ColFam(ksDemo, "Clusters", hasCompositeName = true)    // keys=UTF8Type, names=(UTF8Type, UTF8Type)
}
