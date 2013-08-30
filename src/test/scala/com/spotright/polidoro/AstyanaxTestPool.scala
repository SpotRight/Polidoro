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
import org.specs2.specification._

import com.spotright.polidoro.session.TestableCassConnector

/**
 * Example of attaching an EmbeddedCassandra for testing.
 *
 * Copy this file into src/test/scala/... of your project.
 */
trait AstyanaxTestPool extends Specification with util.LogHelper {

  val cassConnector: TestableCassConnector = SpotAsty

  override
  def map(fs: => Fragments): Fragments = {
    Step {
      cassConnector.specTestInit()
    } ^ fs ^
      Step {
        cassConnector.specTestShutdown()
      }
  }
}
