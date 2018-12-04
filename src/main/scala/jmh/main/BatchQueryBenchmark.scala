/*
 * Copyright (c) 2018 Adobe Systems Inc. All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the
 * property of Adobe Systems Incorporated and its suppliers, if any.
 * The intellectual and technical concepts contained herein are
 * proprietary to Adobe Systems Incorporated and its suppliers and may
 * be covered by U.S. and Foreign Patents, patents in process, and are
 * protected by trade secret or copyright law. Dissemination of this
 * information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Adobe Systems Inc.
 */

package jmh.main

import java.util.concurrent._

import com.microsoft.azure.documentdb._
import jmh.main.config.CosmosSettings
import jmh.main.documentdb.PartitionMetadata
import jmh.main.util.BatchQueryHelper
import org.openjdk.jmh.annotations._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random


@State(Scope.Benchmark)
class BatchQueryBenchmark {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  @Param(Array("10"))
  var executorThreads: Int = 10

  @Param(Array("6"))
  var partitionBatchSize: Int = 6

  @Param(Array("0"))
  var startPartition:Int = 0

  @Param(Array("-1"))
  var endPartition:Int = -1

  @Param(Array("0"))
  var specRetries:Int = 0

  @Param(Array("99999"))
  var specTimeout:Int = 99999

  @Param(Array("Gateway", "DirectHttps"))
  var connectionMode:String = config.cosmosSettings.connectionMode.name()

  @Param(Array("Eventual", "Strong"))
  var consistencyLevel:String = config.cosmosSettings.consistencyLevel.name()

  // Global Variables that will be overridden by setup
  var documentClient:DocumentClient = _
  var documentDbPartitionMetadata: PartitionMetadata = _

  var collectionUrl: String = _
  var idsPerPartition: List[Set[String]] = _

  var executorService: ExecutorService = _
  var enableSpec:Boolean = _


  @Setup(Level.Trial)
  def setupTrial(): Unit = {

    logger.info("setupTrial started ..")

    ////////////////////////  Init the executor service  ////////////////////

    executorService = Executors.newFixedThreadPool(executorThreads)
    val pool = executorService.asInstanceOf[ThreadPoolExecutor]
    //pool.setCorePoolSize(executorThreads)
    //pool.setMaximumPoolSize(executorThreads)
    pool.setKeepAliveTime(120, TimeUnit.SECONDS)
    enableSpec = specRetries > 0

    //////////////////////  Init the DocumentDb client  ////////////////////

    collectionUrl =  s"/dbs/${config.cosmosSettings.dbName}/colls/${config.cosmosSettings.tablePrefix}${config.cosmosSettings.tableName}"

    val connectionPolicy = ConnectionPolicy.GetDefault
    connectionPolicy.setMaxPoolSize(config.cosmosSettings.maxPoolSize)
    connectionPolicy.setConnectionMode(CosmosSettings.getCosmosConnectionMode(connectionMode))

    documentClient = new DocumentClient(
      config.cosmosSettings.api,
      config.cosmosSettings.masterKey,
      connectionPolicy,
      CosmosSettings.getCosmosConsistencyLevel(consistencyLevel)
    )

    documentDbPartitionMetadata = PartitionMetadata(documentClient, collectionUrl)

    //////////////////////  Ready the table  //////////////////////////////


    val dbName = config.cosmosSettings.dbName
    val tablePrefix = config.cosmosSettings.tablePrefix

    /////////////////////  Fetch Ids from each partition  /////////////////

    idsPerPartition = documentDbPartitionMetadata.getIdsPerPartitionParallel(
      documentClient,
      collectionUrl,
      partitionBatchSize,
      executorService)

    if(startPartition < 0 || startPartition > idsPerPartition.size - 1){
      throw new Exception(s"Invalid value for startPartition, must be between 0 and ${idsPerPartition.size - 1}")
    }

    if(endPartition == -1){
      endPartition = idsPerPartition.size-1
      logger.info(s"Setting endPartition to be the last partition (${endPartition})")
    }
    else if(endPartition < -1 || endPartition > idsPerPartition.size - 1){
      throw new Exception(s"Invalid value for endPartition, must be between ${startPartition} and ${idsPerPartition.size - 1}")
    }

    logger.info("setupTrial finished.")
  }


  @TearDown(Level.Trial)
  def doTearDown(): Unit = {
    logger.info("TearDown started..")
    documentClient.close()
    executorService.shutdown()
    executorService.awaitTermination(2000, TimeUnit.MILLISECONDS)
    logger.info("TearDown complete!")
  }

  @Benchmark
  def batchQueryBenchmark(): List[Document] = {
    val keys = idsPerPartition.slice(startPartition, endPartition + 1).flatten.toSet
    queryBatch(keys, enableSpec)
  }

  @Benchmark
  def randomSinglePartitionBenchmark(): List[Document] = {
    val keys = idsPerPartition(Random.nextInt(endPartition))
    queryBatch(keys, enableSpec)
  }

  @Benchmark
  def onePartitionBenchmark(): List[Document] = {
    val keys = idsPerPartition(startPartition)
    queryBatch(keys, enableSpec)
  }


  //helper function for batch queries
  def queryBatch(keys: Set[String], doSpeculativeExecution: Boolean = false): List[Document] = {
    val results = BatchQueryHelper.runBatchQueryParallel(
      keys,
      collectionUrl,
      documentClient,
      documentDbPartitionMetadata,
      executorService,
      enableSpec,
      specTimeout,
      specRetries
    )

    if(results.size != keys.size) {
      throw new Exception(
        s"Result count was ${results.size} not ${keys.size}")
    } else {
      results.foreach{ doc: Document =>
        if(doc.getId.isEmpty) throw new Exception(s"${doc} failed verification")
      }
    }

    results
  }

}

