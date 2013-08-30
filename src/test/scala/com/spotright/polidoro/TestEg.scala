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

class TestEg extends SpecificationWithJUnit with AstyanaxTestPool with util.LogHelper {

  import SpotAsty.DemoFG._
  import CompositeFactory.CF

  "Basic Examples" should {
    val rowkey = "ABC:Test"
    val xName = "lanny"
    val xAge = 44

    // Basic insert
    "insert basic" in  {
      val ops = List(
        Insert(cfUsers \ rowkey \ ("name", xName)),
        Insert(cfUsers \ rowkey \ ("age", xAge)),
        Insert(cfUsers \ rowkey \ ("pfx:1", "")),
        Insert(cfUsers \ rowkey \ ("pfx:2", ""))
      )

      // Ignore returned status info.
      SpotAsty.batch(ops)

      (cfUsers \ rowkey \ "name").get must beSome
    }

    // Composite insert
    "insert composite" in {
      val ops = List(
        Insert(kcfCities \ CF("Texas", "Houston") \ ("resident", xName)),
        Insert(kcfCities \ CF("Texas", "Houston") \ ("region:SE", ""))
      )

      // Explicitly check status for errors.
      SpotAsty.batch(ops).values.forall{_ must beRight}
    }

    step {
      // Synchronization point so data above is available for tests below.
    }

    "get" in {
      val mc = (cfUsers \ rowkey \ "age").get
      mc must beSome
    }

    "get" in {
      val colpath = kcfCities \ CF("Texas", "Houston") \ "resident"
      colpath.rowkey.get(0,SerDes[String]) mustEqual "Texas"
      colpath.rowkey.get(1,SerDes[String]) mustEqual "Houston"

      val mc = colpath.get
      mc must beSome
    }

    "list" in {
      val xs = (cfUsers \ rowkey).list[String]
      Option(xs.getColumnByName("name")) must beSome.which {
        hcol =>
          hcol.getStringValue() mustEqual xName
      }

      xs.getIntegerValue("age", 0) mustEqual xAge
    }

    "list w/ column pred" in {
      val xs = (cfUsers \ rowkey) list List("name")
      Option(xs.getColumnByName("name")) must beSome
      Option(xs.getColumnByName("age")) must beNone
      Option(xs.getColumnByName("pfx:1")) must beNone
      Option(xs.getColumnByName("pfx:2")) must beNone
    }

    "list w/ range pred" in {
      val xs = (cfUsers \ rowkey) list RangePredicate(start="pfx:", end="pfx;")
      Option(xs.getColumnByName("name")) must beNone
      Option(xs.getColumnByName("age")) must beNone
      Option(xs.getColumnByName("pfx:1")) must beSome
      Option(xs.getColumnByName("pfx:2")) must beSome
    }

    "know whether the table is a cf or an scf" in {
      cfUsers.isScf mustEqual false
      kcfCities.isKCf mustEqual true
      scfClusters.isScf mustEqual true
    }
  }
}
