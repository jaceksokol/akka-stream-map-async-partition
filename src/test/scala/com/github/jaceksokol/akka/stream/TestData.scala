package com.github.jaceksokol.akka.stream

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.util.Random

import org.scalacheck.{Arbitrary, Gen}

object TestData {

  case class BufferSize(value: Int) extends AnyVal
  case class Parallelism(value: Int) extends AnyVal

  case class TestKeyValue(key: Int, delay: FiniteDuration, value: String)

  implicit val bufferSizeArb: Arbitrary[BufferSize] = Arbitrary {
    Gen.choose(1, 100).map(BufferSize)
  }
  implicit val parallelismArb: Arbitrary[Parallelism] = Arbitrary {
    Gen.choose(2, 8).map(Parallelism)
  }
  implicit val elementsArb: Arbitrary[Seq[TestKeyValue]] = Arbitrary {
    for {
      totalElements <- Gen.choose(1, 100)
      totalPartitions <- Gen.choose(1, 8)
    } yield {
      generateElements(totalPartitions, totalElements)
    }
  }

  def generateElements(totalPartitions: Int, totalElements: Int): Seq[TestKeyValue] =
    for (i <- 1 to totalElements) yield {
      TestKeyValue(
        key = Random.nextInt(totalPartitions),
        delay = DurationInt(Random.nextInt(20) + 10).millis,
        value = i.toString
      )
    }

  def extractPartition(e: TestKeyValue): Int =
    e.key

  type Operation = TestKeyValue => Future[(Int, String)]

  def asyncOperation(e: TestKeyValue)(implicit ec: ExecutionContext): Future[(Int, String)] =
    Future {
      e.key -> e.value
    }

  def blockingOperation(e: TestKeyValue)(implicit ec: ExecutionContext): Future[(Int, String)] =
    Future {
      blocking {
        Thread.sleep(e.delay.toMillis)
        e.key -> e.value
      }
    }

}
