package com.github.jaceksokol.akka.stream

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Attributes.{Name, SourceLocation}
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import Holder.NotYetThere

class MapAsyncPartition[In, Out](
  parallelism: Int,
  bufferSize: Int,
  extractPartition: In => Partition,
  f: In => Future[Out]
) extends GraphStage[FlowShape[In, Out]] {

  private val in = Inlet[In]("MapAsyncPartition.in")
  private val out = Outlet[Out]("MapAsyncPartition.out")

  override val shape: FlowShape[In, Out] = FlowShape(in, out)

  override def initialAttributes: Attributes = Attributes(Name("MapAsyncPartition")) and SourceLocation.forLambda(f)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider

      private var inProgress: mutable.Map[Partition, Holder[Out]] = _
      private var waiting: mutable.Queue[(Partition, In)] = _

      private val futureCB = getAsyncCallback[Holder[Out]](holder =>
        holder.elem match {
          case Success(_) => pushNextIfPossible()
          case Failure(ex) =>
            holder.supervisionDirectiveFor(decider, ex) match {
              // fail fast as if supervision says so
              case Supervision.Stop => failStage(ex)
              case _                => pushNextIfPossible()
            }
        }
      )

      override def preStart(): Unit = {
        inProgress = mutable.Map()
        waiting = mutable.Queue()
      }

      override def onPull(): Unit =
        pushNextIfPossible()

      override def onPush(): Unit = {
        try {
          val element = grab(in)
          val partition = extractPartition(element)

          if (inProgress.contains(partition) || inProgress.size >= parallelism) {
            waiting.enqueue(partition -> element)
          } else {
            processElement(partition, element)
          }
        } catch {
          case NonFatal(ex) => if (decider(ex) == Supervision.Stop) failStage(ex)
        }

        pullIfNeeded()
      }

      override def onUpstreamFinish(): Unit =
        if (idle()) completeStage()

      private def processElement(partition: Partition, element: In): Unit = {
        val future = f(element)
        val holder = new Holder[Out](NotYetThere, futureCB)
        inProgress.put(partition, holder)

        future.value match {
          case None    => future.onComplete(holder)(scala.concurrent.ExecutionContext.parasitic)
          case Some(v) =>
            // #20217 the future is already here, optimization: avoid scheduling it on the dispatcher and
            // run the logic directly on this thread
            holder.setElem(v)
            v match {
              // this optimization also requires us to stop the stage to fail fast if the decider says so:
              case Failure(ex) if holder.supervisionDirectiveFor(decider, ex) == Supervision.Stop => failStage(ex)
              case _                                                                              => pushNextIfPossible()
            }
        }
      }

      private def pushNextIfPossible(): Unit =
        if (inProgress.isEmpty) {
          drainQueue()
          pullIfNeeded()
        } else if (isAvailable(out)) {
          inProgress.filterInPlace { case (_, holder) =>
            if ((holder.elem eq NotYetThere) || !isAvailable(out)) true
            else {
              holder.elem match {
                case Success(elem) =>
                  if (elem != null) {
                    push(out, elem)
                    pullIfNeeded()
                  } else {
                    // elem is null
                    pullIfNeeded()
                  }

                case Failure(NonFatal(ex)) =>
                  holder.supervisionDirectiveFor(decider, ex) match {
                    // this could happen if we are looping in pushNextIfPossible and end up on a failed future before the
                    // onComplete callback has run
                    case Supervision.Stop =>
                      failStage(ex)
                    case _ =>
                    // try next element
                  }
                case Failure(ex) =>
                  // fatal exception in buffer, not sure that it can actually happen, but for good measure
                  throw ex
              }
              false
            }
          }
          drainQueue()
        }

      private def drainQueue(): Unit = {
        if (waiting.nonEmpty) {
          val todo = waiting
          waiting = mutable.Queue[(Partition, In)]()

          todo.foreach { case (partition, element) =>
            if (inProgress.size >= parallelism || inProgress.contains(partition)) {
              waiting.enqueue(partition -> element)
            } else {
              processElement(partition, element)
            }
          }
        }
      }

      private def pullIfNeeded(): Unit =
        if (isClosed(in) && idle()) completeStage()
        else if (waiting.size < bufferSize && !hasBeenPulled(in)) tryPull(in)
      // else already pulled and waiting for next element

      private def idle(): Boolean =
        inProgress.isEmpty && waiting.isEmpty

      setHandlers(in, out, this)
    }

}
