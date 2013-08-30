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

import java.nio.ByteBuffer

import com.netflix.astyanax.serializers.AbstractSerializer
import com.netflix.astyanax.Keyspace

/**
 * Batchable Operations
 *
 * Operations can be acted on by classes and objects that mix in [[com.spotright.polidoro.session.BatchOps]].
 */
abstract sealed class Operation {
  type Ktype
  type Ntype

  val rpath: RowPathish[Ktype]

  lazy val keyspace: Keyspace = rpath.keyspace
  lazy val cfname: String = rpath.cfname
  lazy val rowkey: Ktype = rpath.rowkey

  val keySD: AbstractSerializer[Ktype]
  val nameSD: AbstractSerializer[Ntype]
}

case class Insert[K: Manifest, N: Manifest, V: Manifest](col: Column[K,N,V]) extends Operation {
  type Ktype = K
  type Ntype = N

  val rpath: RowPathish[Ktype] = col

  val keySD = SerDes[K]
  val nameSD = SerDes[N]
  val valSD = SerDes[V]
}

case class IncrCounter[K: Manifest, N: Manifest](col: Column[K,N,Long]) extends Operation {
  type Ktype = K
  type Ntype = N

  val rpath: RowPathish[Ktype] = col

  val keySD = SerDes[K]
  val nameSD = SerDes[N]
}

case class Delete[K: Manifest, N: Manifest](colpath: ColumnPathish[K,N]) extends Operation {
  type Ktype = K
  type Ntype = N

  val rpath: RowPathish[Ktype] = colpath

  val keySD = SerDes[K]
  val nameSD = SerDes[N]
}

object Delete {
  def apply[K: Manifest](rowpath: RowPath[K]): RowDelete[K, ByteBuffer] = RowDelete[K, ByteBuffer](rowpath)
}

case class RowDelete[K: Manifest, N: Manifest](rowpath: RowPathish[K]) extends Operation {
  type Ktype = K
  type Ntype = N

  val rpath: RowPathish[Ktype] = rowpath

  val keySD = SerDes[K]
  val nameSD = SerDes[N]

  /**
   * Change the name type for the RowDelete Operation.
   */
  def withNtype[T: Manifest]: RowDelete[K, T] = RowDelete[K, T](rowpath)
}
