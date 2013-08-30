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

import scala.collection.JavaConverters._
import scala.sys.process._

import com.spotright.polidoro.session.CassConnector

object Breakpoint {

  /**
   * Invoke ''bin/button.jar'' to display a dialog and wait.
   *
   * E.g.,
   * {{{
   * // In a mapreduce test at some point that wants to look into the embedded cass
   * import com.spotright.hadoop.cass.Breakpoint
   * Breakpoint.forCass(scala = "/opt/scala/current/bin/scala")
   * }}}
   *
   * Using the above you would get a small dialog that displayed the embedded cassandra
   * port and a ''Continue'' button.  You can connect to cass with the port, e.g.,
   * {{{
   * $ /opt/apache/cassandra/bin/cassandra-cli -h localhost -p CASS_PORT
   * }}}
   * and look around.  When done disconnect from Cass and click ''Continue''.
   *
   * @param scala Path to scala.
   * @param port Port to show in dialog's label (if 0 will use `SpotCass.getPorts`).
   */
  def forCass(
               cassConnector: CassConnector = SpotAsty,
               scala: String = "/opt/scala/current/bin/scala",
               port: Int = 0) {
    val cassPort = if (port != 0) port else SpotAsty.testPort().getOrElse(9160)

    val cmd = scala + " bin/button.jar " + cassPort
    cmd.!
  }
}

