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

class TestGet extends SpecificationWithJUnit with AstyanaxTestPool {

  sequential

  import CompositeFactory.CF
  import SpotAsty.DemoFG._

  "Polidoro" should {
    "get" in {
      (cfUsers \ "abc" \ "age").execute{_.putValue(3, null)}

      (cfUsers \ "abc" \ "age").get must beSome
    }

    "not get" in {
      (cfUsers \ "xyz" \ "age").get must beNone
    }

    "get composite" in {
      SpotAsty.batch(
        List(
          Insert(scfClusters \ "Alpha" \ (CF("State", "CO"), ""))
        )
      )

      val got = (scfClusters \ "Alpha" \ CF("State", "CO")) getWith CompStr2.serdes
      got must beSome
    }

    "not get composite" in {
      val got = (scfClusters \ "Alpha" \ CF("State", "TX")) getWith CompStr2.serdes
      got must beNone
    }
  }

}
