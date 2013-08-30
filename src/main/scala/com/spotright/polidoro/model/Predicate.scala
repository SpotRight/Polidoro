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

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import java.nio.ByteBuffer

import com.netflix.astyanax.model.{ColumnSlice, Composite}
import com.netflix.astyanax.serializers.CompositeRangeBuilder

abstract sealed class Predicate[N] {

  def toColumnSlice: ColumnSlice[N]
}

case class ColumnPredicate[N: Manifest](colnames: Seq[N]) extends Predicate[N] {

  def toColumnSlice: ColumnSlice[N] = new ColumnSlice[N](colnames.asJava)
}

object ColumnPredicate {

  def apply[N: Manifest](colname: N, colnames: N*): ColumnPredicate[N] = ColumnPredicate(colname +: colnames)

  // implicits to provide sugar for ColumnPredicate
  implicit def travonce2ColumnPredicate[N: Manifest](colnames: TraversableOnce[N]): ColumnPredicate[N] =
    ColumnPredicate[N](colnames.toSeq)
}

case class RangePredicate[N: Manifest](
                                        start: N = null,
                                        end: N = null,
                                        reversed: Boolean = false,
                                        limit: Int = Int.MaxValue
                                        )
  extends Predicate[N] {
    require(start != null, "start is null")
    require(end != null, "end is null")

  /**
   * Increment the final byte (with overflow) to provide for searching the range inclusively.
   *
   * Given dataset:
   *
   *     * ("alice", 27, "New York")
   *     * ("bob", 32, "New York")
   *     * ("bob", 35, "Seattle")
   *     * ("bobby", 25, "Atlanta")
   *     * ("jack", 27, "Los Angeles")
   *
   * If `end` is a Composite then only the components provided will be queried.  E.g.,
   * {{{
   * import com.spotright.polidoro.model._
   *
   * val se = Compositer.mkComp("bob")
   * val pred = RangePredicate(start=se, end=se).gte
   * }}}
   *
   * Using `pred` above would result in
   *
   *     * ("bob", 32, "New York")
   *     * ("bob", 35, "Seattle")
   *
   * For types other than CompositeType you will get "prefix" semantics where the match will
   * be any value that is prefixed by `end`.
   *
   * @see `lte` for prefix semantics for CompositeType
   */
  protected[model]
  def gte: RangePredicate[ByteBuffer] = RangePredicate.gte(this)

  def toColumnSlice: ColumnSlice[N] = {
    new ColumnSlice[N](start, end)
      .setReversed(reversed)
      .setLimit(limit)
  }
}

object RangePredicate {

  // ToDo - Figure this out.  Really needed or just confusing?
  // This is here to work with Strings but confounds Composites.
  // Proabably clearer to use `gte` on the underlaying CF and `incr` on Strings.
  protected[model]
  def gte[N: Manifest](in: RangePredicate[N]): RangePredicate[ByteBuffer] = {
    val sbb = SerDes[N].toByteBuffer(in.start)
    val ebb = {
      val bytes = SerDes[N].toBytes(in.end)
      val eix = bytes.length-1

      if (bytes(eix) == -1) ByteBuffer.wrap(bytes ++ Array[Byte](0))
      else if (bytes(eix) < 127) {
        bytes(eix) = (bytes(eix) + 1).toByte
        ByteBuffer.wrap(bytes)
      }
      else {
        bytes(eix) = -128: Byte
        ByteBuffer.wrap(bytes)
      }
    }

    RangePredicate[ByteBuffer](sbb, ebb, in.reversed, in.limit)
  }
}

/**
 * Open `end`-ed RangePrefix
 *
 * @note This works with Composite types as well however it's a bit more flexible to work with the
 * Composite directly in that case.
 * {{{
 * val comp = CompositeFactory("a", 7, "b")
 * RangePredicate(start=comp, end=comp.gte)
 * }}}
 */
object PrefixPredicate {

  /** RangePrefix(start=pfx, end=pfx).gte */
  def apply[N: Manifest](pfx: N): RangePredicate[ByteBuffer] = RangePredicate(start=pfx, end=pfx).gte

  /** RangePrefix(start=pfx+startSfx, end=pfx+endSfx).gte */
  def apply(pfx: String, startSfx: String, endSfx: String): RangePredicate[ByteBuffer] =
    RangePredicate(start=pfx+startSfx, end=pfx+endSfx).gte
}

/**
 * Provide a function to modify a CompositeRangeBuilder.
 *
 * The initial strings provided are added using .withPrefix()
 * while the final is added with
 * {{{.greaterThanEquals(s).lessThan(s.incr)}}}
 */
object PrefixPredicateCF {
  /* To get all columns starting with "bob"
   *     ("bob", 32, "New York")
   *     ("bob", 35, "Seattle")
   *     ("bobby", 25, "Atlanta")
   */
  def apply(pfx: String*): CompositeRangeBuilder => CompositeRangeBuilder = {
    crb =>
      pfx.init.foreach(crb.withPrefix(_))

      val last = pfx.last

      crb
      .greaterThanEquals(last)
      .lessThan(last.incr)
  }
}
