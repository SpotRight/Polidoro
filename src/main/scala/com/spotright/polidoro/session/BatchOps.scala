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

import java.util.concurrent.{Future => jucFuture}

import com.netflix.astyanax.{ColumnListMutation, Keyspace, MutationBatch}
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException
import com.netflix.astyanax.connectionpool.OperationResult

import com.spotright.polidoro.model._

trait BatchOps {
  self: CassConnector =>

  final val BATCH_SIZE = 100
  private val batchLogger = org.slf4j.LoggerFactory.getLogger("com.spotright.polidoro.CassConnector.BatchOps")

  /**
   * Execute all Operations provided.
   *
   * Operations can use mixed Keyspaces but the actual changes are decomposed
   * and exected along keyspace boundries.
   */
  def batch(ops: Seq[Operation]): Map[Keyspace,Either[ConnectionException,OperationResult[Void]]] = {
    val mbs = genMutationBatch(ops)

    mbs.map {
      case (k,m) =>
        try k -> Right(m.execute())
        catch {
          case e: ConnectionException =>
            batchLogger.warn(e.getMessage)
            val debugData = new AnyRef { override def toString: String = ops.filter {_.keyspace == k}.mkString("\n") }
            batchLogger.debug("batch exception debug: keyspace={}. === Dataset ===\n{}\n=== /Dataset ===",
              k.getKeyspaceName, debugData)
            k -> Left(e)
        }
    }
  }

  /**
   * Execute all Operations provided Asynchronously.
   */
  def batchAsync(ops: Seq[Operation]): Map[Keyspace,Either[ConnectionException,jucFuture[OperationResult[Void]]]] = {
    val mbs = genMutationBatch(ops)

    mbs.map {
      case (k,m) =>
        try k -> Right(m.executeAsync())
        catch {
          case e: ConnectionException =>
            batchLogger.warn(e.getMessage)
            val debugData = new AnyRef { override def toString: String = ops.filter {_.keyspace == k}.mkString("\n") }
            batchLogger.debug("batchAsync exception debug: keyspace={}. === Dataset ===\n{}\n=== /Dataset ===",
              k.getKeyspaceName, debugData)
            k -> Left(e)
        }
    }
  }

  /**
   * Execute Operations provided in groups of `batch_size`.
   */
  def bunch[K: Manifest, N: Manifest](ops: Seq[Operation], batch_size: Int = BATCH_SIZE) {
    require(batch_size > 0, "batch_size <= 0")
    ops.grouped(batch_size).foreach {
      elts =>
        batch(elts.toSeq)
    }
  }

  def bunchAsync[K: Manifest, N: Manifest](ops: Seq[Operation], batch_size: Int = BATCH_SIZE) {
    require(batch_size > 0, "batch_size <= 0")
    ops.grouped(batch_size).foreach {
      elts =>
        batchAsync(elts.toSeq)
    }
  }

  protected
  def orNull(in: Option[Int]): java.lang.Integer = if (in.isEmpty) null else in.get


  /**
   * Transform Seq[Operation] into Seq[MutationBatch]
   *
   * @note It is fine to mix Operations in differing keyspaces.
   */
  protected
  def genMutationBatch(ops: Seq[Operation]): Map[Keyspace, MutationBatch] = {
    /*
     * This is probably stupidly brittle.  The main problem is having to implement
     * the model using type values rather than type parameters.  Using parameters
     * though means that an Operation is kind * => * => * rather than *.
     */
    val opsByKS = ops.groupBy{_.keyspace}

    opsByKS.map {
      case (ks, ksOps) =>
        val m = ks.prepareMutationBatch()
        val opsByCF = ksOps.groupBy{op => (op.cfname, op.keySD.hashCode, op.nameSD.hashCode)}

        opsByCF.foreach {
          case (_, cfOps) =>
            val hop = cfOps.head
            val cf = new ColumnFamily(hop.cfname, hop.keySD, hop.nameSD)
            val opsByRowkey = cfOps.groupBy{_.rowkey}

            opsByRowkey.foreach {
              case (rowkey, rkOps) =>
                val cm = m.withRow(cf.asInstanceOf[ColumnFamily[Operation#Ktype, hop.Ntype]], rowkey)

                rkOps.foreach {
                  op =>
                    type Kt = op.Ktype
                    type Nt = op.Ntype
                    val cmTyped = cm.asInstanceOf[ColumnListMutation[Nt]]

                  op match {
                    case ins@Insert(col) =>
                      if (col.colval == "" || col.colval == null)
                        cmTyped.putEmptyColumn(col.colname.asInstanceOf[Nt], orNull(col.ttl))
                      else
                        cmTyped.putColumn(
                          col.colname.asInstanceOf[Nt], ins.valSD.toByteBuffer(col.colval), orNull(col.ttl)
                        )

                    case IncrCounter(col) =>
                      cmTyped.incrementCounterColumn(col.colname.asInstanceOf[Nt], col.colval)

                    case Delete(colpath) =>
                      cmTyped.deleteColumn(colpath.colname.asInstanceOf[Nt])

                    case RowDelete(_) =>
                      cm.delete()
                  }
                }
            }
        }

        ks -> m
    }
  }
}
