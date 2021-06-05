package com.github.jaceksokol.akka.stream

import scala.concurrent.{ExecutionContext, Future}

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.{Flow, FlowWithContext, Sink, Source, SourceWithContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OperatorApplicabilitySpec extends AnyFlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  private implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "test-system")
  private implicit val ec: ExecutionContext = system.executionContext

  override protected def afterAll(): Unit = {
    system.terminate()
    system.whenTerminated.futureValue
    super.afterAll()
  }

  private def f(i: Int): Future[Int] =
    Future(i % 2)

  it should "be applicable to a source" in {
    Source
      .single(3)
      .mapAsyncPartition(parallelism = 1)(_.toString)(f)
      .runWith(Sink.seq)
      .futureValue shouldBe Seq(1)
  }

  it should "be applicable to a source with context" in {
    SourceWithContext
      .fromTuples(Source.single(3 -> "A"))
      .mapAsyncPartition(parallelism = 1)(_.toString)(f)
      .runWith(Sink.seq)
      .futureValue shouldBe Seq(1 -> "A")
  }

  it should "be applicable to a flow" in {
    Flow[Int]
      .mapAsyncPartition(parallelism = 1)(_.toString)(f)
      .runWith(Source.single(3), Sink.seq)
      ._2
      .futureValue shouldBe Seq(1)
  }

  it should "be applicable to a flow with context" in {
    val flow =
      FlowWithContext[Int, String]
        .mapAsyncPartition(parallelism = 1)(_.toString)(f)

    SourceWithContext
      .fromTuples(Source.single(3 -> "A"))
      .via(flow)
      .runWith(Sink.seq)
      .futureValue shouldBe Seq(1 -> "A")
  }

}
