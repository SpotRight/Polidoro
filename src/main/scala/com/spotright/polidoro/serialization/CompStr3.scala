package com.spotright.polidoro.serialization

import java.nio.ByteBuffer

import com.netflix.astyanax.annotations.Component
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer

class CompStr3 {
  @Component(ordinal=0) var c1: String = _
  @Component(ordinal=1) var c2: String = _
  @Component(ordinal=2) var c3: String = _
}

object CompStr3 {

  def apply(c1: String, c2: String, c3: String): CompStr3 = {
    val rv = new CompStr3()
    rv.c1 = c1
    rv.c2 = c2
    rv.c3 = c3
    rv
  }

  def unapply(in: CompStr3): Option[(String,String,String)] = Some((in.c1, in.c2, in.c3))

  def unapply(in: ByteBuffer): Option[(String,String,String)] = {
    def nonull(s: String) = if (s == null) "" else s
    val CompStr3(c1, c2, c3) = serdes.fromByteBuffer(in.duplicate())
    Some((nonull(c1), nonull(c2), nonull(c3)))
  }

  val serdes = new AnnotatedCompositeSerializer[CompStr3](classOf[CompStr3])
}
