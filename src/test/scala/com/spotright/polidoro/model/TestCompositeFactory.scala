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

import org.specs2.mutable._

import com.netflix.astyanax.model.AbstractComposite.{ComponentEquality => CE}
import com.netflix.astyanax.model.Composite

import com.spotright.polidoro.serialization.CompStr2

class TestCompositeFactory extends SpecificationWithJUnit {

  import CompositeFactory.CF

  "CompositeFactory" should {
    "create composites" in {
      val cf = CF("answer", 42)

      cf.get(0, SerDes[String]) mustEqual "answer"
      cf.get(1, SerDes[Int]) mustEqual 42
    }

    "create GREATER_THAN_EQUAL composites" in {
      val cf = CF("answer", 42).gte

      val bb = Composite.toByteBuffer(cf)

      val len = bb.getShort
      for (i <- 1 to len) {
        bb.get()
      }
      bb.get()

      val len2 = bb.getShort
      for (i <- 1 to len2) {
        bb.get()
      }

      bb.get() mustEqual CE.GREATER_THAN_EQUAL.toByte
    }

    "create LESS_THAN_EQUAL composites" in {
      val cf = CF("answer", 42).lte

      val bb = Composite.toByteBuffer(cf)

      val len = bb.getShort
      for (i <- 1 to len) {
        bb.get()
      }
      bb.get()

      val len2 = bb.getShort
      for (i <- 1 to len2) {
        bb.get()
      }

      bb.get mustEqual CE.LESS_THAN_EQUAL.toByte
    }

    "unapply a bytebuffer with a CompStr2" in {
      val bb = Composite.toByteBuffer(CF("hello", "world"))
      val CompStr2(x, y) = bb

      x mustEqual "hello"
      y mustEqual "world"
    }
  }
}
