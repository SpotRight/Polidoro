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

import com.spotright.polidoro.AstyanaxTestPool
import com.spotright.polidoro.SpotAsty.DemoFG._

class TestColFam extends SpecificationWithJUnit with AstyanaxTestPool {

  "ColFam" should {
    "know about isScf" in {
      cfUsers.isCf mustEqual true
      cfUsers.isScf mustEqual false
      cfUsers.isKCf mustEqual false
      cfUsers.isKScf mustEqual false

      kcfCities.isCf mustEqual false
      kcfCities.isScf mustEqual false
      kcfCities.isKCf mustEqual true
      kcfCities.isKScf mustEqual false

      scfClusters.isCf mustEqual false
      scfClusters.isScf mustEqual true
      scfClusters.isKCf mustEqual false
      scfClusters.isKScf mustEqual false
    }
  }
}
