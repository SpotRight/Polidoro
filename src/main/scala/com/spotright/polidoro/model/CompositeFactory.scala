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
package model

import scala.language.implicitConversions

import java.nio.ByteBuffer

import com.netflix.astyanax.model.AbstractComposite.{ComponentEquality => CE}
import com.netflix.astyanax.model.Composite

/**
 * Builder for Composites
 *
 * The simplest way to use this object is
 * {{{
 * import com.spotright.polidoro.model._
 *
 * // In some later scope
 * import CompositeFactory.CF
 * val composite = CF("TX", 77043, "Houston")
 * }}}
 *
 * The CF application uses the CV{N} case class constructors and end up returning a
 * [[com.netflix.astyanax.model.Composite]] which has a trait applied which allows
 * the `gte` and `lte` methods on the CV{N} case classes to be called.  This is very
 * helpful in range queries such as
 * {{{
 * val start = CF("Houston")
 * val needsWork = (scfCities \ city).list(start = start, end = start.gte)
 * }}}
 *
 * Please note that these helper methods do not modify the Composite components.  There are String helpers in
 * the model package to increment and decrement the last character of a string.  In particular for `lte` to do
 * what you expect you need to `decr` the final string component.
 *
 *@see [[com.spotright.polidoro.model.PrefixPredicateCF]]
 */
object CompositeFactory {

  val CF = this

  def apply[A: Manifest](a: A): Composite with CV1Shadow[A] = CV1(a).toComp

  def apply[A: Manifest, B: Manifest](a: A, b: B): Composite with CV2Shadow[A,B] = CV2(a, b).toComp

  def apply[A: Manifest, B: Manifest, C: Manifest](a: A, b: B, c: C): Composite with CV3Shadow[A,B,C] =
    CV3(a, b, c).toComp

  def apply[A: Manifest, B: Manifest, C: Manifest, D: Manifest](a: A, b: B, c: C, d: D)
  : Composite with CV4Shadow[A,B,C,D] = CV4(a, b, c, d).toComp

  def apply[A: Manifest, B: Manifest, C: Manifest, D: Manifest, E: Manifest](a: A, b: B, c: C, d: D, e: E)
  : Composite with CV5Shadow[A,B,C,D,E] = CV5(a,b,c,d,e).toComp
}

/**
 * A CV{N}Shadow gets mixed into Composite in CV{N}'s `toComp` method.
 */
trait CV1Shadow[A] {
  val cv: CV1[A]
  def gte: Composite with CV1Shadow[A] = cv.gte.toComp
  def lte: Composite with CV1Shadow[A] = cv.lte.toComp
}

case class CV1[A: Manifest](
                                       a: A,
                                       eql: CE = CE.EQUAL)
{
  self =>

  def gte: CV1[A] = this.copy(eql = CE.GREATER_THAN_EQUAL)
  def lte: CV1[A] = this.copy(eql = CE.LESS_THAN_EQUAL)

  def toComp: Composite with CV1Shadow[A] = {
    val rv = new Composite() with CV1Shadow[A] {
      val cv = self
    }
    rv.addComponent(a, SerDes[A], eql)
    rv
  }
}

object CV1 {
  implicit def cv1ToComposite[A](in: CV1[A]): Composite = in.toComp
}

trait CV2Shadow[A,B] {
  val cv: CV2[A,B]
  def gte: Composite with CV2Shadow[A,B] = cv.gte.toComp
  def lte: Composite with CV2Shadow[A,B] = cv.lte.toComp
}

case class CV2[A: Manifest, B: Manifest](
                                                 a: A, b: B,
                                                 eql: CE = CE.EQUAL)
{
  self =>

  def gte: CV2[A,B] = this.copy(eql = CE.GREATER_THAN_EQUAL)
  def lte: CV2[A,B] = this.copy(eql = CE.LESS_THAN_EQUAL)

  def toComp: Composite with CV2Shadow[A,B] = {
    val rv = new Composite() with CV2Shadow[A,B] {
      val cv = self
    }
    rv.addComponent(a, SerDes[A])
    rv.addComponent(b, SerDes[B], eql)
    rv
  }
}

object CV2 {
  implicit def cv2ToComposite[A,B](in: CV2[A,B]): Composite = in.toComp
}

trait CV3Shadow[A,B,C] {
  val cv: CV3[A,B,C]
  def gte: Composite with CV3Shadow[A,B,C] = cv.gte.toComp
  def lte: Composite with CV3Shadow[A,B,C] = cv.lte.toComp
}

case class CV3[A: Manifest, B: Manifest, C: Manifest](
                                                              a: A, b: B, c: C,
                                                              eql: CE = CE.EQUAL)
{
  self =>

  def gte: CV3[A,B,C] = this.copy(eql = CE.GREATER_THAN_EQUAL)
  def lte: CV3[A,B,C] = this.copy(eql = CE.LESS_THAN_EQUAL)

  def toComp: Composite with CV3Shadow[A,B,C] = {
    val rv = new Composite() with CV3Shadow[A,B,C] {
      val cv = self
    }
    rv.addComponent(a, SerDes[A])
    rv.addComponent(b, SerDes[B])
    rv.addComponent(c, SerDes[C], eql)
    rv
  }
}

object CV3 {
  implicit def cv3ToComposite[A,B,C](in: CV3[A,B,C]): Composite = in.toComp
}

trait CV4Shadow[A,B,C,D] {
  val cv: CV4[A,B,C,D]
  def gte: Composite with CV4Shadow[A,B,C,D] = cv.gte.toComp
  def lte: Composite with CV4Shadow[A,B,C,D] = cv.lte.toComp
}

case class CV4[A: Manifest, B: Manifest, C: Manifest, D: Manifest](
                                                                           a: A, b: B, c: C, d: D,
                                                                           eql: CE = CE.EQUAL)
{
  self =>

  def gte: CV4[A,B,C,D] = this.copy(eql = CE.GREATER_THAN_EQUAL)
  def lte: CV4[A,B,C,D] = this.copy(eql = CE.LESS_THAN_EQUAL)

  def toComp: Composite with CV4Shadow[A,B,C,D] = {
    val rv = new Composite() with CV4Shadow[A,B,C,D] {
      val cv = self
    }
    rv.addComponent(a, SerDes[A])
    rv.addComponent(b, SerDes[B])
    rv.addComponent(c, SerDes[C])
    rv.addComponent(d, SerDes[D], eql)
    rv
  }
}

object CV4 {
  implicit def cv4ToComposite[A,B,C,D](in: CV4[A,B,C,D]): Composite = in.toComp
}

trait CV5Shadow[A,B,C,D,E] {
  val cv: CV5[A,B,C,D,E]
  def gte: Composite with CV5Shadow[A,B,C,D,E] = cv.gte.toComp
  def lte: Composite with CV5Shadow[A,B,C,D,E] = cv.lte.toComp
}

case class CV5[A: Manifest, B: Manifest, C: Manifest, D: Manifest, E: Manifest](
                                                                                        a: A, b: B, c: C, d: D, e: E,
                                                                                        eql: CE = CE.EQUAL)
{
  self =>

  def gte: CV5[A,B,C,D,E] = this.copy(eql = CE.GREATER_THAN_EQUAL)
  def lte: CV5[A,B,C,D,E] = this.copy(eql = CE.LESS_THAN_EQUAL)

  def toComp: Composite with CV5Shadow[A,B,C,D,E] = {
    val rv = new Composite() with CV5Shadow[A,B,C,D,E] {
      val cv = self
    }
    rv.addComponent(a, SerDes[A])
    rv.addComponent(b, SerDes[B])
    rv.addComponent(c, SerDes[C])
    rv.addComponent(d, SerDes[D])
    rv.addComponent(e, SerDes[E], eql)
    rv
  }
}

object CV5 {
  implicit def cv5ToComposite[A,B,C,D,E](in: CV5[A,B,C,D,E]): Composite = in.toComp
}
