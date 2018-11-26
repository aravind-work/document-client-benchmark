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

import java.util.UUID
import java.util.concurrent._

import com.microsoft.azure.documentdb._
import jmh.main.config.CosmosSettings
import jmh.main.documentdb.PartitionMetadata
import jmh.main.util.{BatchHelper, BatchQueryHelper, ParallelBatchHelper}
import org.openjdk.jmh.annotations._
import org.slf4j.{Logger, LoggerFactory}

@State(Scope.Benchmark)
class IngestBenchmark {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  @Param(Array("2"))
  var numOfInputIdentities: Int = 2

  @Param(Array("10"))
  var executorThreads: Int = 10

  /*
  @Param(Array("Gateway", "DirectHttps"))
  var connectionMode:String = config.cosmosSettings.connectionMode.name()

  @Param(Array("Eventual", "Strong"))
  var consistencyLevel:String = config.cosmosSettings.consistencyLevel.name()
  */

  // Global Variables that will be overridden by setup
  var documentClient:DocumentClient = _
  var routingDocumentDbPartitionMetadata: PartitionMetadata = _
  var graphDocumentDbPartitionMetadata: PartitionMetadata = _

  var routingCollectionUrl: String = _
  var graphCollectionUrl: String = _

  var executorService: ExecutorService = _


  @Setup(Level.Trial)
  def setupTrial(): Unit = {

    logger.info("setupTrial started ..")

    ////////////////////////  Init the executor service  ////////////////////

    executorService = Executors.newFixedThreadPool(executorThreads)
    val pool = executorService.asInstanceOf[ThreadPoolExecutor]
    pool.setCorePoolSize(executorThreads)
    pool.setKeepAliveTime(120, TimeUnit.SECONDS)

    //////////////////////  Init the DocumentDb client  ////////////////////

    routingCollectionUrl =  s"/dbs/${config.cosmosSettings.dbName}/colls/${config.cosmosSettings.tablePrefix}routing"
    graphCollectionUrl =  s"/dbs/${config.cosmosSettings.dbName}/colls/${config.cosmosSettings.tablePrefix}graph"

    val connectionPolicy = ConnectionPolicy.GetDefault
    connectionPolicy.setMaxPoolSize(config.cosmosSettings.maxPoolSize)
    connectionPolicy.setConnectionMode(config.cosmosSettings.connectionMode)

    documentClient = new DocumentClient(
      config.cosmosSettings.api,
      config.cosmosSettings.masterKey,
      connectionPolicy,
      config.cosmosSettings.consistencyLevel
    )

    routingDocumentDbPartitionMetadata = PartitionMetadata(documentClient, routingCollectionUrl)
    graphDocumentDbPartitionMetadata = PartitionMetadata(documentClient, graphCollectionUrl)

    //////////////////////  Ready the table  //////////////////////////////


    val dbName = config.cosmosSettings.dbName
    val tablePrefix = config.cosmosSettings.tablePrefix

    /////////////////////  Fetch Ids from each partition  /////////////////
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
  def ingestIdentitiesBenchmark(): Unit = {
    val identities = createIdentities(numOfInputIdentities)

    // Reading routing and graph entries to mimic ingestion flow
    // In this particular use case, we shouldn't be reading anything back
    val routingMap = getRoutingEntries(identities)
    val graphMap: Map[String, Graph] = {
      if(routingMap.nonEmpty) {
        val graphMap = getGraphEntries(routingMap.values.map{routing => routing.graphId}.toSet)
        graphMap
      } else Map.empty[String, Graph]
    }

    if(graphMap.nonEmpty) {
      logger.warn(s"Existing Graphs: ${graphMap.size}")
    }

    // In this use case, we are just creating new graphs with generated identities
    val graph = Graph(identities = identities)
    val routingEntries = identities.map{ identity =>
      Routing(identity, graph.id)
    }

    persistData(routingEntries, Set(graph))
  }

  def createIdentities(n: Int): Set[String] = {
    Seq.fill(n)(UUID.randomUUID().toString).toSet
  }

  def getRoutingEntries(ids: Set[String]): Map[String, Routing] = {
    val routingDocuments = queryBatch(ids, routingCollectionUrl, routingDocumentDbPartitionMetadata)
    if(routingDocuments.nonEmpty) {
      routingDocuments.map{ document =>
        val id = document.getId
        val graphId  = document.getString("graphId")
        id -> Routing(id, graphId)
      }.toMap
    } else Map.empty[String, Routing]
  }

  def getGraphEntries(ids: Set[String]): Map[String, Graph] = {
    val routingDocuments = queryBatch(ids, graphCollectionUrl, graphDocumentDbPartitionMetadata)
    if(routingDocuments.nonEmpty) {
      routingDocuments.map{ document =>
        val id = document.getId
        val identities  = document.getString("identities")
        id -> Graph(id, identities.split(",").toSet)
      }.toMap
    } else Map.empty
  }

  def persistData(updatedRouting: Set[Routing], updatedGraphs: Set[Graph]): Unit = {
    val latch = new CountDownLatch(updatedRouting.size + updatedGraphs.size)

    //Submit all Persist Tasks in Parallel based on batch of operations
    val persistGraphsOps = ParallelBatchHelper[Graph, Unit](updatedGraphs.toSeq, executorService, latch)(persistGraph)
    val persistRoutingOps = ParallelBatchHelper[Routing, Unit](updatedRouting.toSeq, executorService, latch)(persistRouting)

    //Wait for all Persist Tasks to Finish
    latch.await()
  }

  //helper function for batch queries
  def queryBatch(keys: Set[String], collectiontUrl: String, documentDbPartitionMetaData: PartitionMetadata): List[Document] = {
    val results = BatchQueryHelper.runBatchQueryParallel(
      keys = keys,
      collectionUrl = collectiontUrl,
      documentClient = documentClient,
      documentDbPartitionMetadata = documentDbPartitionMetaData,
      service = executorService,
      doSpeculativeExecution = false,
      speculativeTimeout = 0,
      speculativeRetries = 0
    )
    results
  }

  def persistRouting(routing: Routing): Unit = {
    val key = routing.id
    val requestOptions = new RequestOptions()
    requestOptions.setPartitionKey(new PartitionKey(key))

    val jsonString =
      s"""{"id":"$key","graphId":"${routing.graphId}"}""".stripMargin
    val document = new Document(jsonString)

    try {
      documentClient.upsertDocument(routingCollectionUrl, document, requestOptions, true)
    } catch {
      case e: DocumentClientException =>
        logger.error(s"Exception in Writing $routing - ${e.getError}")
    }
  }

  def persistGraph(graph: Graph): Unit = {
    val key = graph.id
    val requestOptions = new RequestOptions()
    requestOptions.setPartitionKey(new PartitionKey(key))

    val identities = graph.identities.mkString(",")
    val jsonString =
      s"""{"id":"$key","identities":"$identities"}""".stripMargin
    val document = new Document(jsonString)

    try {
      documentClient.upsertDocument(graphCollectionUrl, document, requestOptions, true)
    } catch {
      case e: DocumentClientException =>
        logger.error(s"Exception in Writing $graph - ${e.getError}")
    }

  }
}

case class Routing(id: String, graphId: String)

case class Graph(id: String = UUID.randomUUID().toString, identities: Set[String])