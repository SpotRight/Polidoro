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

/**
 * Model of Cassandra Column Families, Predicates, and Operations.
 */
package object model {

  type ColFamish = com.spotright.polidoro.modelish.ColFamish
  type RowPathish[K] = com.spotright.polidoro.modelish.RowPathish[K]
  type ColumnPathish[K, N] = com.spotright.polidoro.modelish.ColumnPathish[K, N]
  type Columnish[K, N, V] = com.spotright.polidoro.modelish.Columnish[K, N, V]

  /**
   * Increment the last character of a string.
   *
   * @note No range checking is made so be careful of boundry conditions.
   */
  implicit
  class StrIncr(in: String) {
    def incr: String = if (in == null || in.isEmpty) in else in.init + (in.last + 1).toChar
  }

  /**
   * Decrement the last character of a string.
   *
   * @note No range checking is made so be careful of boundry conditions.
   */
  implicit
  class StrDecr(in: String) {
    def decr: String = if (in == null || in.isEmpty) in else in.init + (in.last - 1).toChar
  }

  /**
   * Decorate a String with a method to provide a ByteBuffer
   */
  implicit
  class StrUTF8ByteBuffer(in: String) {
    def utf8BB: java.nio.ByteBuffer = java.nio.ByteBuffer.wrap(in.getBytes("UTF8"))
  }
}
