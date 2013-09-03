package com.spotright.polidoro.session

import org.specs2.mutable._

import com.netflix.astyanax.connectionpool.Host

class TestContextConainerImpl extends SpecificationWithJUnit {

  "ContextContainerImpl" should {
    "extend ContextContainer" in {
      val got = new ContextContainerImpl(
        ContextContainerConfig("test", List(new Host("localhost", 9160)))
      )

      got.isInstanceOf[ContextContainer] mustEqual true
    }
  }

}
