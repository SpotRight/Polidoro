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

import com.netflix.astyanax.model.{Column => AstyColumn, ColumnList, ColumnFamily}
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException
import com.netflix.astyanax.serializers.{CompositeRangeBuilder, AnnotatedCompositeSerializer}

import com.spotright.polidoro.serialization.{CompStr2, CompStr3}

/**
 * Cassandra Column Family Row
 *
 * A RowPath encapsulates the columns under a given key.
 *
 * =Operations=
 *
 * Any RowPath can be used to recover the columns under the key possibly
 * restrained by Column or Range predicate.  The `list[N]` method can be used to query
 * on column names of type N.
 * {{{
 * // All columns.  Requires type annotation for column name deserializer.
 * val cfr = (cfUsers \ username).list[String]
 * val maybeName = Option(cfr.getColumn("name")).map{hcol => SerDes[String].fromByteBuffer(hcol.value)}
 *
 * // named columns - equivalent constructions
 * import com.spotright.polidoro.model._
 *
 * val cfr = (cfUsers \ username) list ColumnPredicate(List("name", "age"))
 * val cfr = (cfUsers \ username) list ColumnPredicate("name", "age")
 * val cfr = (cfUsers \ username) list List("name", "age")    // any TraversableOnce
 *
 * // ranged columns
 * import com.spotright.polidoro.model._
 *
 * val cfr = (cfUsers \ username) list RangePredicate(startStr, endStr, reversed=false, limit=Int.MaxValue)
 * val cfr = (cfUsers \ username) list(startStr, endStr, reversed = false, limit = Int.MaxValue)
 * }}}
 *
 * A RowPath can get a single composite column using getWith.  There are shortcuts (get) if getting a
 * [[com.spotright.polidoro.serialization.CompStr2]] or [[com.spotright.polidoro.serialization.CompStr3]]
 * {{{
 * val col = (scfClusters \ cassCluster).get(CF("Colorado", "name"))
 * }}}
 *
 * A RowPath can query Composite Name column lists with listWith and listWithRange/using.  See TestCompColname.scala.
 *
 * A RowPath can be used to construct a Column (including columns with composite column names).
 * {{{
 * val col = cfUsers \ username \ ("employer", "SpotRight")
 *
 * // Note the final representation in Cass is different from above.
 * import ColumnFactory.CF
 * val compCol = scfClusters \ cassCluster \ (CF("Colorado", "name"), "MilkyWay")
 * }}}
 *
 * You can also construct a ColumnPath.  These can be used for get (non-Composite) and in Delete Operations.
 * {{{
 * val colpath = cfUsers \ username \ "location"
 *
 * // Composite ColPath
 * import CompositeFactory.CF
 * val scfClusters = cfOutlets \ cassCluster \ CF("Colorado", "name")
 * }}}
 */
case class RowPath[K: Manifest](colfam: ColFam, rowkey: K) extends RowPathish[K] {
  def \[N: Manifest](name: N): ColumnPath[K,N] = ColumnPath[K,N](this, name)
  def \[N: Manifest, V: Manifest](name: N, value: V): Column[K,N,V] = Column(ColumnPath(this, name), value)
  def \[N: Manifest, V: Manifest](name: N, value: V, ttl: Int): Column[K,N,V] =
    Column(ColumnPath(this, name), value, Option(ttl))

  val keyspace = colfam.keyspace
  val cfname = colfam.cfname

  def list[N: Manifest]: ColumnList[N] = {
    keyspace.prepareQuery(new ColumnFamily(cfname, SerDes[K], SerDes[N]))
      .getKey(rowkey)
      .execute()
      .getResult
  }

  def listWith[N](acsN: AnnotatedCompositeSerializer[N]): ColumnList[N] = {
    keyspace.prepareQuery(new ColumnFamily(cfname, SerDes[K], acsN))
      .getKey(rowkey)
      .execute()
      .getResult
  }

  def listWithRange[N](acsN: AnnotatedCompositeSerializer[N]): ListWithRangeRunner[N] = ListWithRangeRunner(acsN)

  case class ListWithRangeRunner[N](acsN: AnnotatedCompositeSerializer[N]) {
    def using(body: CompositeRangeBuilder => CompositeRangeBuilder): ColumnList[N] = {
      val bbr = body(acsN.buildRange()).build()

      keyspace.prepareQuery(new ColumnFamily(cfname, SerDes[K], acsN))
        .getKey(rowkey)
        .withColumnRange(bbr)
        .execute()
        .getResult
    }
  }

  def listPred[N: Manifest](pred: Predicate[N]): ColumnList[N] = {
    keyspace.prepareQuery(new ColumnFamily(cfname, SerDes[K], SerDes[N]))
      .getKey(rowkey)
      .withColumnSlice(pred.toColumnSlice)
      .execute()
      .getResult
  }

  // Specialize on the predicate for implicit conversions
  def list[N: Manifest](pred: ColumnPredicate[N]): ColumnList[N] = listPred(pred)
  def list[N: Manifest](pred: RangePredicate[N]): ColumnList[N] = listPred(pred)

  def list[N: Manifest](start: N, end: N, reversed: Boolean = false, limit: Int = Int.MaxValue)
  : ColumnList[N] = list(RangePredicate(start, end, reversed, limit))
}

object RowPath {

  def colfamL[K: Manifest]: Lens[RowPath[K],ColFam] = lensg(rp => cf => rp.copy(colfam = cf), _.colfam)

  def keyspaceL[K: Manifest] = ColFam.keyspaceL <=< colfamL[K]
  def cfnameL[K: Manifest] = ColFam.cfnameL <=< colfamL[K]
  def rowkeyL[K: Manifest]: Lens[RowPath[K],K] = lensg(rp => k => rp.copy(rowkey = k), _.rowkey)
}
