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

package com.spotright

/**
 * Polidoro, the (not so evil) twin brother of Cassandra.
 *
 * Polidoro is a light sugar over Astyanax using much the same model as
 * [[https://github.com/shorrockin/cascal/wiki/ Chris Shorrockin's Cascal]]
 * with slight changes where required.
 *
 * The primary goal is to provide the simple insert, delete, list and get operations
 * from Cascal.  For anything more complex you can use Astyanax directly.
 *
 * The column family model supported by Polidoro breaks down as below.
 *
 * ==ColFam==
 *
 * Holds the keyspace and column family name.
 *
 * ==RowPath==
 *
 * Adds a rowkey onto the ColFam.  Supports the `list` call to slice over the row.
 *
 * ==ColumnPath==
 *
 * Targets a specific column by name.  Supports the `get` call to optionally return
 * the column.  Also supports [[com.netflix.astyanax.ColumnMutation]] calls (although
 * see Operations below for batch support).
 *
 * ==Column==
 *
 * Assigns a value to a ColumnPath.
 *
 * ==Composites==
 *
 * A [[com.spotright.polidoro.model.CompositeFactory]] is available to make creating Composites easier.
 *
 * ==Operations==
 *
 * An [[com.spotright.polidoro.model.Operation]] is either an [[com.spotright.polidoro.model.Insert]],
 * [[com.spotright.polidoro.model.IncrCounter]],
 * [[com.spotright.polidoro.model.Delete]], or a [[com.spotright.polidoro.model.RowDelete]].
 * Operations can be consumed by the `bunch` or `batch` methods in the [[com.spotright.polidoro.SpotAsty]]
 * object.
 *
 * ==Predicates==
 *
 * The [[com.spotright.polidoro.model.ColumnPredicate]], [[com.spotright.polidoro.model.RangePredicate]],
 * and [[com.spotright.polidoro.model.PrefixPredicate]] concepts are supported.  Additional sugar has been
 * provided to make these structures easier to work with.  E.g.,
 * {{{
 * val maybeCol = (cfOutlets \ url) list List("a", "b", "c")
 *
 * val rangeCols = (cfOutlets \ url) list ("a", "z".incr)
 * }}}
 *
 * =Testing=
 *
 * The SpotAsty object supports the `specTestInit` call for testing which loads an embedded cassandra
 * server.  A [[com.spotright.polidoro.AstyanaxTestPool]] trait is available as a template to embedding
 * the daemon in tests that use specs2.
 */
package object polidoro {

  val SerDes = com.spotright.polidoro.serialization.SerDes
}
