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

package com.spotright.polidoro.testing.keyspaces

import com.netflix.astyanax.Cluster
import com.netflix.astyanax.ddl.ColumnFamilyDefinition
import com.netflix.astyanax.serializers.ComparatorType._

/**
 * Define a ColumnFamily given a Cluster.
 */
object ColumnFamilyDef {

  def apply(
             cfName: String,
             keyType: String = BYTESTYPE.tyn,
             nameType: String = BYTESTYPE.tyn,
             valueType: String = BYTESTYPE.tyn,
             columnDefs: Map[String, ColumnDef] = Map.empty[String,ColumnDef]
             ): Cluster => ColumnFamilyDefinition = (cluster: Cluster) => {
    val cdef = cluster.makeColumnFamilyDefinition()
    val keyValidator = if (keyType.startsWith("(")) "CompositeType" + keyType else keyType
    val nameComparator = if (nameType.startsWith("(")) "CompositeType" + nameType else nameType

    cdef.setName(cfName)
    cdef.setKeyValidationClass(keyValidator)
    cdef.setComparatorType(nameComparator)
    cdef.setDefaultValidationClass(valueType)

    columnDefs.foreach {
      case (name, ColumnDef(vc, ki)) =>
        cluster.makeColumnDefinition()
          .setName(name)
          .setValidationClass(vc)
          .setKeysIndex(ki)
    }

    cdef
  }
}
