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

package com.spotright.polidoro.model


import org.specs2.mutable._

import com.netflix.astyanax.model.ColumnList

import com.spotright.polidoro._
import com.spotright.polidoro.serialization.CompStr2

class TestRangePredicate extends SpecificationWithJUnit with AstyanaxTestPool {
  sequential

  import CompositeFactory.CF
  import SpotAsty.DemoFG._

  "RangePredicate" should {

    def workDone(v: String): String = "workDone:" + v
    def workTbd(v: String): String = "workTbd:" + v
    def workToDo(v: String): String = "workToDo:" + v

    val workDoneCooked = workDone("")
    val workTbdCooked = workTbd("")
    val workToDoCooked = workToDo("")

    // set up Cass data
    step {
      val ops = List(
        Insert(scfClusters \ "a" \ (CF("a", "a"), 1))
      , Insert(scfClusters \ "a" \ (CF("a", "b"), 1))
      , Insert(scfClusters \ "a" \ (CF("a", "c"), 1))
      , Insert(scfClusters \ "a" \ (CF("a", "d"), 1))
        , Insert(scfClusters \ "a" \ (CF("a", "da"), 1))
        , Insert(scfClusters \ "a" \ (CF("a", "db"), 1))
        , Insert(scfClusters \ "a" \ (CF("a", "dba"), 1))
        , Insert(scfClusters \ "a" \ (CF("a", "dc"), 1))
        , Insert(scfClusters \ "a" \ (CF("a", "dd"), 1))
        , Insert(scfClusters \ "a" \ (CF("a", "de"), 1))
      , Insert(scfClusters \ "a" \ (CF("a", "e"), 1))
      //
      , Insert(scfClusters \ "a" \ (CF("b", "a"), 1))
      , Insert(scfClusters \ "a" \ (CF("b", "b"), 1))
      , Insert(scfClusters \ "a" \ (CF("b", "c"), 1))
      , Insert(scfClusters \ "a" \ (CF("b", "d"), 1))
        , Insert(scfClusters \ "a" \ (CF("b", "da"), 1))
        , Insert(scfClusters \ "a" \ (CF("b", "db"), 1))
        , Insert(scfClusters \ "a" \ (CF("b", "dc:a"), 1))
        , Insert(scfClusters \ "a" \ (CF("b", "dc:b"), 1))
        , Insert(scfClusters \ "a" \ (CF("b", "dd"), 1))
        , Insert(scfClusters \ "a" \ (CF("b", "de"), 1))
      , Insert(scfClusters \ "a" \ (CF("b", "e"), 1))
      //
      , Insert(scfClusters \ "z" \ (CF("workDone:test", ""), 1))
      , Insert(scfClusters \ "z" \ (CF("workTbd:test", ""), 1))
      , Insert(scfClusters \ "z" \ (CF("workToDo:test", ""), 1))
      )

      SpotAsty.batch(ops)
    }

    def ??(o: String): String = if (o != null) o else ""

    def namesToString(xs: ColumnList[CompStr2]): List[String] = {
      (for (i <- 0 until xs.size()) yield {
        val CompStr2(c1, c2) = xs.getColumnByIndex(i).getName
        "CF(" + c1 + "," + ??(c2) + ")"
      })(collection.breakOut)
    }

    "basic" in {
      // RangePredicate results in ColumnList[Composite with CV{N}Shadow]

      val xs = (scfClusters \ "a") listWithRange CompStr2.serdes using {
        _
        .withPrefix("a")
        .greaterThanEquals("a")
        .lessThanEquals("c")
      }
      println("basic" + " :" + xs.size)
      println(namesToString(xs).mkString("\n"))
      xs.size() mustEqual 3
    }

    "basic lte" in {
      val xs = (scfClusters \ "a") listWithRange CompStr2.serdes using {
        _
        .withPrefix("a")
        .greaterThanEquals("a")
        .lessThan("c")
      }
      println("basic lte" + " :" + xs.size)
      println(namesToString(xs).mkString("\n"))
      xs.size() mustEqual 2
    }

    "example of PrefixPredicateCF" in {
      val xs = (scfClusters \ "a") listWithRange CompStr2.serdes using PrefixPredicateCF("b", "dc:")
      xs.size() mustEqual 2
    }

    "work markers" in {
      // Note how the work marker ending in ":" needs to have final char incremented to capture all values
      val xs = (scfClusters \ "z") listWithRange CompStr2.serdes using {
        _
        .greaterThanEquals(workDoneCooked)
        .lessThan(workToDoCooked.incr)
      }

      println("work markers" + " :" + xs.size)
      println(namesToString(xs).mkString("\n"))

      xs.size() mustEqual 3
    }

    "work markers - incorrect" in {
      val xs = (scfClusters \ "z") listWithRange CompStr2.serdes using {
        _
        .greaterThanEquals(workDoneCooked)
        .lessThan(workToDoCooked)
      }

      println("work markers - incorrect" + " :" + xs.size)
      println(namesToString(xs).mkString("\n"))

      xs.size() mustEqual 2
    }
  }
}
