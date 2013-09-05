package com.spotright.polidoro.serialization

import org.specs2.mutable._

import com.netflix.astyanax.model.Composite

import com.spotright.polidoro.model.CompositeFactory.CF

class TestCompStr2 extends SpecificationWithJUnit {

  "CompStr2" should {
    "round-trip and empty string" in {
      val CompStr2(c1, c2) = SerDes[Composite].toByteBuffer(CF("A", ""))

      c1 mustEqual "A"
      c2 mustEqual ""
    }
  }
}
