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

package com.spotright.polidoro.testing
package keyspaces

import com.netflix.astyanax.serializers.ComparatorType

object Demonstration extends KeyspaceLoader {

  import ComparatorType._

  val keyspaceName = "Demonstration"

  cdefs += ColumnFamilyDef("Users", keyType = UTF8TYPE.tyn, nameType = UTF8TYPE.tyn)

  cdefs += ColumnFamilyDef("Cities", keyType = "(UTF8Type, UTF8Type)")
  cdefs += ColumnFamilyDef("Clusters", nameType = "(UTF8Type, UTF8Type)")
}
