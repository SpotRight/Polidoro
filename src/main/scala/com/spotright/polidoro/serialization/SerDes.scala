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

package com.spotright.polidoro.serialization

import com.netflix.astyanax.model.Composite
import com.netflix.astyanax.serializers._

/**
 * Serializers/Deserializers
 *
 * Used (mostly) internally by Polidoro.
 * {{{
 * // A string serializer
 * val ss = SerDes[String]
 *
 * // A composite serializer
 * val cs = SerDes[Composite]
 *
 * // ColumnFamilyTemplate[String,Composite]
 * val tmpl = new ThriftColumnFamilyTemplate[String,Int](ksp, cfName, SerDes[String], SerDes[Composite])
 * }}}
 *
 * @note No idea why but if you use a [[com.netflix.astyanax.serailizers.StringSerializer]] in the map()
 *       call of a hadoop Mapper the map() will infinite loop.  To get around this create an asStr() and/or
 *       asBB() which call SerDes[String] in the helper object and use those.  For whatever reason that
 *       works just fine.
 */
object SerDes {

  object Types {
    class Def[C](implicit desired: Manifest[C]) {
      def unapply[X](c: X)(implicit m: Manifest[X]): Option[C] = {
        def sameArgs = desired.typeArguments.zip(m.typeArguments).forall {
          case (desired,actual) => desired >:> actual
        }
        if (desired >:> m && sameArgs) Some(c.asInstanceOf[C]) else None
      }
    }

    val BoolT = manifest[Boolean].toString
    val CharT = manifest[Char].toString
    val IntT = manifest[Int].toString
    val LongT = manifest[Long].toString
    val FloatT = manifest[Float].toString
    val DoubleT = manifest[Double].toString
    val StringT = manifest[String].toString

    val ByteBuffer = manifest[java.nio.ByteBuffer].toString
    val ByteArray = new Def[Array[Byte]]
    val Composite = classOf[Composite].getName
  }

  def apply[A: Manifest]: AbstractSerializer[A] = {
    // Because of the CV{N}Shadow[A] mixins from CompositeFactory we only view the major type component
    // This is probably brittle.
    val typeStr = manifest[A].toString.takeWhile{_ != ' '}

    // attempt to order by most likely call
    val rv = typeStr match {
      case Types.StringT => StringSerializer.get()
      case Types.Composite => CompositeSerializer.get()
      case Types.ByteBuffer => ByteBufferSerializer.get()
      case Types.DoubleT => DoubleSerializer.get()
      case Types.BoolT => BooleanSerializer.get()
      case Types.CharT => CharSerializer.get()
      case Types.IntT  => IntegerSerializer.get()
      case Types.LongT => LongSerializer.get()
      case Types.FloatT => FloatSerializer.get()
      case Types.ByteArray(_) => BytesArraySerializer.get()
      case _ => sys.error("unknown type of Serializer: " + typeStr)
    }

    rv.asInstanceOf[AbstractSerializer[A]]
  }

  def get[A: Manifest]: Option[AbstractSerializer[A]] = {
    try Some(apply[A])
    catch {
      case e: Exception => None
    }
  }
}
