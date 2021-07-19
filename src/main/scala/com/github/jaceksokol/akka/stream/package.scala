package com.github.jaceksokol.akka

import scala.concurrent.{ExecutionContext, Future}

import akka.stream.scaladsl.{Flow, FlowWithContext, Source, SourceWithContext}

package object stream {

  val DefaultBufferSize = 10

  private def extractPartitionWithCtx[In, Ctx, Partition](extract: In => Partition)(tuple: (In, Ctx)): Partition =
    extract(tuple._1)

  private def fWithCtx[In, Out, Ctx](f: In => Future[Out])(tuple: (In, Ctx)): Future[(Out, Ctx)] =
    f(tuple._1).map(_ -> tuple._2)(ExecutionContext.parasitic)

  implicit class SourceExtension[In, +Mat](source: Source[In, Mat]) {
    def mapAsyncPartition[T, Partition](parallelism: Int, bufferSize: Int = DefaultBufferSize)(extractPartition: In => Partition)(
      f: In => Future[T]
    ): Source[T, Mat] =
      source.via(new MapAsyncPartition[In, T, Partition](parallelism, bufferSize, extractPartition, f))
  }

  implicit class SourceWithContextExtension[In, +Ctx, +Mat](flow: SourceWithContext[In, Ctx, Mat]) {
    def mapAsyncPartition[T, Partition](parallelism: Int, bufferSize: Int = DefaultBufferSize)(extractPartition: In => Partition)(
      f: In => Future[T]
    ): SourceWithContext[T, Ctx, Mat] =
      flow.via(
        new MapAsyncPartition[(In, Ctx), (T, Ctx), Partition](
          parallelism,
          bufferSize,
          extractPartitionWithCtx(extractPartition),
          fWithCtx(f)
        )
      )
  }

  implicit class FlowExtension[In, +Out, +Mat](flow: Flow[In, Out, Mat]) {
    def mapAsyncPartition[T, Partition](parallelism: Int, bufferSize: Int = DefaultBufferSize)(extractPartition: Out => Partition)(
      f: Out => Future[T]
    ): Flow[In, T, Mat] =
      flow.via(new MapAsyncPartition[Out, T, Partition](parallelism, bufferSize, extractPartition, f))
  }

  implicit class FlowWithContextExtension[In, +Out, Ctx, +Mat](flow: FlowWithContext[In, Ctx, Out, Ctx, Mat]) {
    def mapAsyncPartition[T, Partition](parallelism: Int, bufferSize: Int = DefaultBufferSize)(extractPartition: Out => Partition)(
      f: Out => Future[T]
    ): FlowWithContext[In, Ctx, T, Ctx, Mat] =
      flow.via(
        new MapAsyncPartition[(Out, Ctx), (T, Ctx), Partition](
          parallelism,
          bufferSize,
          extractPartitionWithCtx(extractPartition),
          fWithCtx(f)
        )
      )
  }

}
