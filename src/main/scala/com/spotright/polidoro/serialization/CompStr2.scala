package com.spotright.polidoro.serialization

import java.nio.ByteBuffer

import com.netflix.astyanax.annotations.Component
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer

class CompStr2 {
  @Component(ordinal=0) var c1: String = _
  @Component(ordinal=1) var c2: String = _
}

object CompStr2 {

  def apply(c1: String, c2: String): CompStr2 = {
    val rv = new CompStr2()
    rv.c1 = c1
    rv.c2 = c2
    rv
  }

  def unapply(in: CompStr2): Option[(String,String)] = Some((in.c1, in.c2))

  def unapply(in: ByteBuffer): Option[(String,String)] = {
    def nonull(s: String) = if (s == null) "" else s
    val CompStr2(c1, c2) = serdes.fromByteBuffer(in.duplicate())
    Some((nonull(c1), nonull(c2)))
  }

  val serdes = new AnnotatedCompositeSerializer[CompStr2](classOf[CompStr2])
}
