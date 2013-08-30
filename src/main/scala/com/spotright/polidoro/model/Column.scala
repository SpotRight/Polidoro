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

/**
 * Cassandra Column
 */
case class Column[K: Manifest, N: Manifest, V: Manifest](colpath: ColumnPath[K,N], colval: V, ttl: Option[Int] = None)
  extends Columnish[K,N,V] {
  require(colval != null, "colval is null")

  def !(ttl: Int): Column[K,N,V] = this.copy(ttl = Option(ttl))

  val keyspace = colpath.rowpath.colfam.keyspace
  val cfname = colpath.rowpath.colfam.cfname
  val rowkey = colpath.rowpath.rowkey
  val colname = colpath.colname
}

object Column {

  def colpathL[K: Manifest,N: Manifest,V: Manifest]: Lens[Column[K,N,V],ColumnPath[K,N]] =
   lensg(c => cp => c.copy(colpath = cp), _.colpath)

  def keyspaceL[K: Manifest,N: Manifest,V: Manifest] = ColumnPath.keyspaceL[K,N] <=< colpathL[K,N,V]
  def cfnameL[K: Manifest,N: Manifest,V: Manifest] = ColumnPath.cfnameL[K,N] <=< colpathL[K,N,V]
  def rowkey[K: Manifest,N: Manifest,V: Manifest] = ColumnPath.rowkeyL[K,N] <=< colpathL[K,N,V]
  def colnameL[K: Manifest,N: Manifest,V: Manifest] = ColumnPath.colnameL[K,N] <=< colpathL[K,N,V]
  def colval[K: Manifest,N: Manifest,V: Manifest]: Lens[Column[K,N,V],V] =
    lensg(c => v => c.copy(colval = v), _.colval)
}

