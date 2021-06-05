package com.github.jaceksokol.akka.stream

import scala.Numeric.Implicits._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.{Sink, Source}
import com.github.jaceksokol.akka.stream.TestData._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec

object MapAsyncPartitionBenchmark extends AnyFlatSpec with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(
    timeout = 10 seconds,
    interval = 100 millis
  )

  implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "benchmark")
  implicit val ec: ExecutionContext = system.executionContext

  def mean[T : Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T : Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)
    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T : Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

  sealed trait OperationDefinition {
    def operation: Operation
  }
  case object AsyncOperation extends OperationDefinition {
    val operation: Operation = asyncOperation
  }
  case object BlockingOperation extends OperationDefinition {
    val operation: Operation = blockingOperation
  }

  case class TestCase(
    bufferSize: BufferSize,
    parallelism: Parallelism,
    elements: Seq[TestKeyValue],
    operation: OperationDefinition
  ) {
    override def toString: Partition = s"$bufferSize, $parallelism, Elements(${elements.size}, $operation)"
  }

  private val TestCases = Seq(
    TestCase(
      BufferSize(1),
      Parallelism(1),
      elements = generateElements(8, 100000),
      AsyncOperation
    ),
    TestCase(
      BufferSize(10),
      Parallelism(1),
      elements = generateElements(8, 100000),
      AsyncOperation
    ),
    TestCase(
      BufferSize(10),
      Parallelism(4),
      elements = generateElements(8, 100000),
      AsyncOperation
    ),
    TestCase(
      BufferSize(10),
      Parallelism(8),
      elements = generateElements(8, 100000),
      AsyncOperation
    ),
    TestCase(
      BufferSize(10),
      Parallelism(1),
      elements = generateElements(8, 100),
      BlockingOperation
    ),
    TestCase(
      BufferSize(10),
      Parallelism(4),
      elements = generateElements(8, 100),
      BlockingOperation
    ),
    TestCase(
      BufferSize(10),
      Parallelism(8),
      elements = generateElements(8, 100),
      BlockingOperation
    )
  )

  it should "run benchmarks" in {
    println("Warming up...")
    TestCases
      .map(testCase => testCase.copy(elements = testCase.elements.take(30)))
      .foreach(test(repeat = 2, print = false))

    println("Metering...")
    TestCases.foreach(test(repeat = 10, print = true))
  }

  private def test(repeat: Int, print: Boolean)(testCase: TestCase): Unit = {
    import testCase._

    var times: List[Long] = Nil

    for (_ <- 1 to repeat) {
      val start = System.nanoTime()

      Source
        .fromIterator(() => elements.iterator)
        .mapAsyncPartition(parallelism.value, bufferSize.value)(extractPartition)(operation.operation)
        .runWith(Sink.ignore)
        .futureValue

      val stop = System.nanoTime()
      val time = (stop - start) / 1000000

      times = times :+ time
    }
    if (print) {
      println(
        f"mapAsyncPartition[$testCase]:\tMean=${mean(times)}%.3f ms\tStdDev=${stdDev(times)}%.3f ms\t==================="
      )
    }
  }

}
