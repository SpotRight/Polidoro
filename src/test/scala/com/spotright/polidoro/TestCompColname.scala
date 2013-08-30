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

import org.specs2.mutable._

import com.spotright.polidoro.model._
import com.spotright.polidoro.serialization.CompStr2

class TestCompColname extends SpecificationWithJUnit with AstyanaxTestPool {

  sequential

  import SpotAsty.DemoFG._
  import CompositeFactory.CF

  val clusterName = "ABC"

  "Polidoro Composite Columns" should {
    "load test data" in {
      val ops = List(
        Insert(scfClusters \ clusterName \ (CF("alpha", "apple"), "fred")),
        Insert(scfClusters \ clusterName \ (CF("beta", "ball"), "wilma")),
        Insert(scfClusters \ clusterName \ (CF("beta", "balloon"), "barney")),
        Insert(scfClusters \ clusterName \ (CF("betaBeta", "charlie"), "betty"))
      )

      SpotAsty.batch(ops).values.forall{_ must beRight}
    }

    "get" in {
      val mcol2 = (scfClusters \ clusterName \ CF("alpha", "apple")) getWith CompStr2.serdes
      mcol2 must beSome.like {
        case c =>
          val CompStr2(c1, c2) = c.getName
          c1 mustEqual "alpha"
          c2 mustEqual "apple"
          c.getStringValue mustEqual "fred"
      }
    }

    "slice query" in {
      // You must specify the key comparator type for list of all columns.
      val xs = (scfClusters \ clusterName) listWith CompStr2.serdes

      xs.size() must be_>(0)
    }

    "slice query (gte)" in {
      // This form will find key CF("alpha", ...)
      val cnComp = CF("alpha")
      val xs = (scfClusters \ clusterName) listWithRange CompStr2.serdes using {_.greaterThanEquals("alpha")}

      xs.size() must be_>(0)
    }

    "slice query (lte)" in {
      // This form will find key CF("beta.*", ...)
      val start = CF("beta")
      val end = CV1("beta".incr).lte

      val xs = (scfClusters \ clusterName) listWithRange CompStr2.serdes using {
        _
          .greaterThanEquals("beta")
          .lessThan("beta".incr)
      }

      xs.size() mustEqual 3
    }

    "use PrefixPredicateCF" in {
      //val ys = (scfClusters \ term).list(PrefixPredicateCF("beta"))
      val xs = (scfClusters \ clusterName) listWithRange CompStr2.serdes using PrefixPredicateCF("beta")

      xs.size() mustEqual 3
    }
  }
}
