package jmh.main.util

import java.util.concurrent.{Callable, FutureTask}
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent._

/**
  * An asynchronously executed task which can be cancelled
  * before or during its execution.
  *
  * @param body the task to execute
  * @param ec   the context in which to execute the task
  * @param promise the promise to complete with the task's result
  * @tparam T the return type of `body`
  */
final class CancellableTask[T] private(body: => T, ec: ExecutionContext, promise:Promise[T]) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  @inline
  private def callable[A](body: => A): Callable[A] = new Callable[A] {override def call() = body}

  // create a FutureTask that completes the provided promise with the result obtained by running the callable
  private val task:FutureTask[T] = new FutureTask[T](callable(body)) {
    override def done() = if (!promise.isCompleted) {
      promise.trySuccess(task.get())
    }
  }

  //run the FutureTask
  ec.execute(task)


  def future: Future[T] = promise.future
  def cancel(mayInterruptIfRunning: Boolean): Boolean = task.cancel(mayInterruptIfRunning)
  def isCancelled: Boolean = task.isCancelled
}

object CancellableTask {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  def apply[T](body: => T)(implicit ec: ExecutionContext): CancellableTask[T] = new CancellableTask(body, ec, Promise[T]())
  def apply[T](body: => T, promise: Promise[T])(implicit ec: ExecutionContext): CancellableTask[T] = {
    new CancellableTask(body, ec, promise)
  }
}