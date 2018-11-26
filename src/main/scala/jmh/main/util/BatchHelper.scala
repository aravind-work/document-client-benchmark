package jmh.main.util

import java.util.concurrent._
import scala.util.{Failure, Success, Try}

object BatchHelper {

  def batchWithTiming[T, V](inputs: Seq[T], executorService: ExecutorService)
                           (callback:T => V): Map[T, Try[V]] = {
    if (inputs.nonEmpty) {
      val latch = new CountDownLatch(inputs.size)

      val futuresMap = submitBatch(inputs, executorService, latch)(callback)

      latch.await()

      val results = handleBatch(futuresMap)
      results
    } else Map.empty
  }

  def submitBatch[T, V](inputs: Seq[T], executorService: ExecutorService, latch: CountDownLatch)
                       (callback:T => V): Map[T, FutureTask[Try[V]]] = {
    if(inputs.nonEmpty) {
      val futuresMap = inputs.map{ input =>
        val future = new FutureTask[Try[V]](
          new Callable[Try[V]] {
            def call: Try[V] = {
              try {
                Try(callback(input))
              } finally {
                latch.countDown()
              }
            }
          }
        )

        executorService.submit(future)
        input -> future
      }.toMap
      futuresMap
    } else Map.empty
  }


  def handleBatch[T, V](futuresMap: Map[T, FutureTask[Try[V]]]): Map[T, Try[V]] = {
    futuresMap.map { case (key, future) =>
      val futureResult = Try(future.get)
      futureResult match {
        case Success(result) => key -> result
        case Failure(e) => key -> Failure[V](e)
      }
    }
  }
}

case class ParallelBatchHelper[T,V](input: Seq[T], executorService: ExecutorService, latch: CountDownLatch)(fn: T => V) {
  val futureMap: Map[T, FutureTask[Try[V]]] = submitBatch()

  def submitBatch(): Map[T, FutureTask[Try[V]]] = {
    BatchHelper.submitBatch(input, executorService, latch)(fn)
  }

  def getBatchResults(): Map[T, Try[V]] = {
    BatchHelper.handleBatch(futureMap)
  }
}