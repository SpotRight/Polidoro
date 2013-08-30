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
package model

import scalaz._
import scalaz.Lens._

import com.netflix.astyanax.Keyspace

/**
 * Cassandra Column Family
 *
 * A column family consists of a keyspace and a family name.  Column families in Polidoro
 * only model CFs (although CompositeType keys and columnNames are supported).  If you need to access
 * an SCF you will need to roll the Astyanax code yourself.
 *
 * By convention the value a ColFam is assigned to should use the following prefixes
 *
 *     cf-   basic column family
 *     scf-  composite columnNames
 *     kcf-  composite keys
 *     kscf- composite keys and columnNames
 *
 * =Operations=
 *
 * A ColFam can be used to construct a [[com.spotright.polidoro.model.RowPath]].  RowPath construction
 * can also handle CompositeType keys.
 * {{{
 * // RowPath[String]
 * val rp = cfUsers \ username
 *
 * // RowPath[Composite]
 * import CompositeFactory.CF
 * val rpc = scfCities \ CF(state, city)    // RowPath[Composite]
 * }}}
 *
 * A colfam can answer if it has composite keys or columnNames and shortcuts for the various
 * combinations are also defined.
 */
case class ColFam(keyspace: Keyspace, cfname: String, hasCompositeKey: Boolean = false, hasCompositeName: Boolean = false)
  extends ColFamish {

  require(cfname.matches("\\w+"), "Invalid cfname <" + cfname + ">.")

  def \[K: Manifest](key: K): RowPath[K] = RowPath(this, key)

  def isCf: Boolean = !hasCompositeKey && !hasCompositeName
  def isScf: Boolean = !hasCompositeKey && hasCompositeName
  def isKCf: Boolean = hasCompositeKey && !hasCompositeName
  def isKScf: Boolean = hasCompositeKey && hasCompositeName
}

object ColFam {

  val keyspaceL: Lens[ColFam, Keyspace] = lensg(cf => ks => cf.copy(keyspace = ks), _.keyspace)
  val cfnameL: Lens[ColFam, String] = lensg(cf => cfn => cf.copy(cfname = cfn), _.cfname)
}
