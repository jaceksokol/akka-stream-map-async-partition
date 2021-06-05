package com.github.jaceksokol.akka.stream

import scala.util.{Failure, Try}
import scala.util.control.NoStackTrace

import akka.stream.Supervision
import akka.stream.stage.AsyncCallback

private object Holder {
  val NotYetThere: Failure[Nothing] = Failure(new Exception with NoStackTrace)
}

private final class Holder[T](var elem: Try[T], val cb: AsyncCallback[Holder[T]]) extends (Try[T] => Unit) {

  // To support both fail-fast when the supervision directive is Stop
  // and not calling the decider multiple times (#23888) we need to cache the decider result and re-use that
  private var cachedSupervisionDirective: Option[Supervision.Directive] = None

  def supervisionDirectiveFor(decider: Supervision.Decider, ex: Throwable): Supervision.Directive = {
    cachedSupervisionDirective match {
      case Some(d) => d
      case _ =>
        val d = decider(ex)
        cachedSupervisionDirective = Some(d)
        d
    }
  }

  def setElem(t: Try[T]): Unit = {
    elem = t
  }

  override def apply(t: Try[T]): Unit = {
    setElem(t)
    cb.invoke(this)
  }
}
