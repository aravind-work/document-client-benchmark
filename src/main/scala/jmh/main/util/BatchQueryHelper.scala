package jmh.main.util

import java.util.TimerTask
import java.util.concurrent.{Callable, ExecutorService, TimeUnit}

import com.microsoft.azure.documentdb.{Document, DocumentClient, FeedOptions}
import jmh.main.documentdb.PartitionMetadata
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration.{Duration, FiniteDuration}
import collection.JavaConverters._

object BatchQueryHelper {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  // Used for scheduling the speculative tasks
  val timer = new java.util.Timer(true)

  def runBatchQueryParallel(keys: Set[String],
                            collectionUrl: String,
                            documentClient: DocumentClient,
                            documentDbPartitionMetadata: PartitionMetadata,
                            service: ExecutorService,
                            doSpeculativeExecution: Boolean = false,
                            speculativeTimeout: Int,
                            speculativeRetries:Int): List[Document] = {
    val ec:ExecutionContext = ExecutionContext.fromExecutor(service)

    keys.isEmpty match {

      case true => List.empty
      case false => {
        val partitionKeyIdsMap = documentDbPartitionMetadata.groupIdsByPartition(keys.toSeq)

        val callables = partitionKeyIdsMap.map { case (partition, keySet) =>
          new Callable[List[Document]] {
            def call():List[Document] =
            {
              val options = new FeedOptions
              options.setPageSize(-1)
              options.setPartitionKeyRangeIdInternal(partition)

              val sqlQuery = generateQuery(keySet)

              val documentList =
                documentClient
                  .queryDocuments(collectionUrl, sqlQuery, options)
                  .getQueryIterable.toList

              documentList.asScala.toList
            }
          }
        }

        val futures = callables.map(lambda => {
          doSpeculativeExecution match {
            case true =>  withSpeculativeRetries[List[Document]](duration.FiniteDuration.apply(speculativeTimeout, TimeUnit.MILLISECONDS), speculativeRetries)({lambda.call()}, "")(ec)
            case false => Future[List[Document]]{ lambda.call() }(ec)
          }
        })

        //wait till all futures complete
        val documents = futures.flatMap(Await.result[List[Document]](_, Duration.Inf))

        documents.toList
      }
    }
  }


  def withSpeculativeRetries[A](delay: FiniteDuration, maxSpeculativeExecutions: Int)
                               (f: => A, debugId:String)
                               (ec: ExecutionContext): Future[A] = {

    // Promise that's re-used by all attempts. First attempt that completes the promise 'wins'
    val promise = Promise[A]()
    // Keep track of all the speculative tasks that have been launched
    val speculativeTasks = new mutable.ArrayBuffer[CancellableTask[A]]()
    // Keep track of all the timer tasks
    val timerTasks = new mutable.ArrayBuffer[TimerTask]()

    var attempt = 0
    // Launch the first attempt
    val originalTask: CancellableTask[A] = CancellableTask[A](f, promise)(ec)

    (1 to maxSpeculativeExecutions).map(_ * delay).map(executionTimeout => {
      // Create a timer task to schedule the speculative executions
      val task = new TimerTask() {
        override def run(): Unit = { // task to run goes here
          if (!promise.isCompleted) {
            attempt = attempt + 1
            //logger.warn(s"${Thread.currentThread().getName} :: debugId = ${debugId} :: Launching speculative " +
            //  s"retry(${attempt}) after ${delay.toMillis * attempt}")
            // Launch the speculative task attempt, use the provided ec
            val sTask = CancellableTask(f, promise)(ec)
            speculativeTasks += sTask
          }
        }
      }
      // Schedule the speculative tasks
      timer.schedule(task, executionTimeout.toMillis)
      timerTasks += task
    })


    // First Task to finish will complete the promise.
    promise.future.onComplete { _ =>

      if(attempt > 0) {
        //logger.warn(s"${Thread.currentThread().getName} :: debugId = ${debugId} :: " +
          //s"Attempts made = ${attempt}")
      }

      //Got result, cancel everything
      timerTasks.foreach(task => {task.cancel()})
      speculativeTasks.foreach(task => {task.cancel(true)})
      originalTask.cancel(true)

    }(ExecutionContext.global) //Run on global fork-join pool

    promise.future
  }


  def generateQuery(keys: Set[String]): String = {
    val keysWithQuotes = keys.map(key => s""""$key"""")
    val inClause = keysWithQuotes.mkString(",")
    val sqlQuery = s"SELECT * FROM c WHERE c.id IN ($inClause)"
    sqlQuery
  }
}
