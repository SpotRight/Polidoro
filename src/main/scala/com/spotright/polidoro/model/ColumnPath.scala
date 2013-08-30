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

import java.util.concurrent.{Future => jucFuture}

import scalaz._
import scalaz.Lens._

import com.netflix.astyanax.model.{Column => AstyColumn, Composite, ColumnFamily}
import com.netflix.astyanax.{Execution, ColumnMutation}
import com.netflix.astyanax.connectionpool.exceptions.{NotFoundException, ConnectionException}
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer

/**
 * Cassandra Column Path
 *
 * A ColumnPath represents the full path to a given column.  Any ColumnPath (or substructure) can
 * use `get` to optionally fetch the column (although recent changes mean that to get a Composite Column
 * you need to use RowPath to do so).  Additionally `execute` and `executeAsync` expose
 * the Astyanax operations available on a [[com.netflix.astyanax.ColumnMutation]].
 *
 * =Operations=
 *
 * The `get` operation returns the column if it exists.  Use `getWith` to name the serializer for composite columns.
 * {{{
 * val maybeCol = (cfUsers \ username \ ("name").get
 * val needsWork = maybeCol.map{_.getValue}.getOrElse("-unknown-")
 *
 * // Short-cut for get with a default
 * val needsWork = (cfUsers \ username \ ("name").getOrElse("-unknown-")
 * }}}
 *
 * The `execute` and `executeAsync` operations allow for modifying the Column found at this ColumnPath.
 *
 * A ColumnPath can be used to construct a Column.
 * {{{
 * val col = colpath \ 7
 * }}}
 */
case class ColumnPath[K: Manifest, N: Manifest](rowpath: RowPath[K], colname: N) extends ColumnPathish[K,N] {
  require(!SerDes[N].toBytes(colname).isEmpty, "colname serializes as empty")
  def \[V: Manifest](colval: V): Column[K,N,V] = Column(this, colval)
  def \[V: Manifest](colval: V, ttl: Int): Column[K,N,V] = Column(this, colval, Option(ttl))

  val keyspace = rowpath.colfam.keyspace
  val cfname = rowpath.colfam.cfname
  val rowkey = rowpath.rowkey

  /**
   * Optionally returns a [[com.netflix.astyanax.model.Column]] if it exists at this ColumnPath.
   */
  def get: Option[AstyColumn[N]] = {
    try Some(
      keyspace.prepareQuery(new ColumnFamily(cfname, SerDes[K], SerDes[N]))
        .getKey(rowkey)
        .getColumn(colname)
        .execute()
        .getResult
    )
    catch {
      case e: NotFoundException => None
    }
  }

  /**
   * Optionally returns a [[com.netflix.astyanax.model.Column]] if it exists at this ColumnPath.
   *
   * Note that the `colname` and serializer type products must agree.  E.g., both (String, String)
   * or both (String, Int, String).
   */
  def getWith[M](acsM: AnnotatedCompositeSerializer[M]): Option[AstyColumn[M]] = {
    val cn = acsM.fromByteBuffer(SerDes[N].toByteBuffer(colname))

    try Some(
      keyspace.prepareQuery(new ColumnFamily(cfname, SerDes[K], acsM))
        .getKey(rowkey)
        .getColumn(cn)
        .execute()
        .getResult
    )
    catch {
      case e: NotFoundException => None
    }
  }

  /**
   * Return the value at this ColumnPath or a default if no column exists in Cassandra.
   *
   * The column could exist and have an empty value.  If you need to check for that
   * use
   * {{{
   * // Assuming the value is a String.
   * colPath.get.flatMap{c => if (c.hasValue) Some(c.getStringValue) else None}.getOrElse(default)
   * }}}
   */
  def getOrElse[V: Manifest](default: => V): V = get.map{_.getValue(SerDes[V])}.getOrElse(default)

  /**
   * Executes a [[com.netflix.astyanax.ColumnMutation]] based on this ColumnPath.
   *
   * You should pass in a function which will take the ColumnMutation to an Execution[Void].  E.g.,
   * {{{
   * // `null` here is the ttl
   * (cfUsers \ username \ "age").execute {_.putValue(3, null)}
   * }}}
   *
   * The Execution[Void] will then be run.
   */
  def execute(body: ColumnMutation => Execution[Void]): Either[ConnectionException, OperationResult[Void]] = {
    val m = keyspace.prepareColumnMutation(new ColumnFamily(cfname, SerDes[K], SerDes[N]), rowkey, colname)
    val ex = body(m)
    try Right(ex.execute())
    catch {
      case e: ConnectionException =>
        Left(e)
    }
  }

  /**
   * Executes a [[com.netflix.astyanax.ColumnMutation]] based on this ColumnPath asynchronously.
   *
   * You should pass in a function which will take the ColumnMutation to an Execution[Void].  E.g.,
   * {{{
   * // `null` here is the ttl
   * (cfUsers \ username \ "age").executeAsync {_.putValue(3, null)}
   * }}}
   *
   * The Execution[Void] will then be run asynchronously resulting in a [[java.util.concurrent.Future]].
   */
  def executeAsync(body: ColumnMutation => Execution[Void])
  : Either[ConnectionException, jucFuture[OperationResult[Void]]] = {
    val m = keyspace.prepareColumnMutation(new ColumnFamily(cfname, SerDes[K], SerDes[N]), rowkey, colname)
    val ex = body(m)
    try Right(ex.executeAsync())
    catch {
      case e: ConnectionException =>
        Left(e)
    }
  }
}

object ColumnPath {

  def rowpathL[K: Manifest,N: Manifest]: Lens[ColumnPath[K,N], RowPath[K]] =
    lensg(cp => rp => cp.copy(rowpath = rp), _.rowpath)

  def keyspaceL[K: Manifest,N: Manifest] = RowPath.keyspaceL[K] <=< rowpathL[K,N]
  def cfnameL[K: Manifest,N: Manifest] = RowPath.cfnameL[K] <=< rowpathL[K,N]
  def rowkeyL[K: Manifest,N: Manifest] = RowPath.rowkeyL[K] <=< rowpathL[K,N]
  def colnameL[K: Manifest,N: Manifest]: Lens[ColumnPath[K,N],N] =
    lensg(cp => cn => cp.copy(colname = cn), _.colname)
}
