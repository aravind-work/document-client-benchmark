package jmh.main.documentdb


import com.microsoft.azure.documentdb.DocumentClient
import com.microsoft.azure.documentdb.DocumentClientException
import com.microsoft.azure.documentdb.FeedOptions
import com.microsoft.azure.documentdb.PartitionKeyDefinition
import com.microsoft.azure.documentdb.PartitionKeyRange
import com.microsoft.azure.documentdb.RequestOptions
import com.microsoft.azure.documentdb.internal.routing.CollectionRoutingMap
import com.microsoft.azure.documentdb.internal.routing.InMemoryCollectionRoutingMap
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal
import com.microsoft.azure.documentdb.internal.routing.Range
import java.util.Collections
import java.util.concurrent.{Callable, ExecutorService, TimeUnit}

import org.apache.commons.lang3.tuple.ImmutablePair

import scala.collection.JavaConversions._

@throws[DocumentClientException]
case class PartitionMetadata(collectionRoutingMap: CollectionRoutingMap[Boolean],
                             partitionKeyRangeIds : List[String],
                             partitionKeyDefinition: PartitionKeyDefinition,
                             collectionLink: String) {

  def getIdsPerPartitionParallel(client: DocumentClient,
                                 collectionLink: String,
                                 itemsPerPartition: Int,
                                 executorService: ExecutorService): List[Set[String]] = {
    val pkRanges = client
      .readPartitionKeyRanges(collectionLink, new FeedOptions())
      .getQueryIterable.toList.toList

    val futures = pkRanges.map(pkRange => {
      new Callable[Set[String]] {
        override def call(): Set[String] = {
          val feedOptions = new FeedOptions
          feedOptions.setPageSize(-1)
          feedOptions.setPartitionKeyRangeIdInternal(pkRange.getId)

          val query = s"SELECT TOP $itemsPerPartition * FROM c"
          val documents =
            client.queryDocuments(collectionLink, query, feedOptions).getQueryIterable.toList
          val ids = documents.map(document => document.getId).toSet
          ids
        }
      }
    }).map(executorService.submit(_))

    futures.map(future =>
      future.get(5 ,TimeUnit.SECONDS)
    )
  }


  def groupIdsByPartition(ids: Seq[String]): Map[String, Set[String]] = {
    ids
      .map(id => {
        val partitionKeyValue = PartitionKeyInternal.fromObjectArray(Collections.singletonList(id), true)
        val effectivePartitionKey = partitionKeyValue.getEffectivePartitionKeyString(this.partitionKeyDefinition, true)
        val partitionRangeId = this.collectionRoutingMap.getRangeByEffectivePartitionKey(effectivePartitionKey).getId
        (partitionRangeId, id)})
      .groupBy(_._1).mapValues(_.map(_._2)).mapValues(_.toSet)
  }
}


object PartitionMetadata {
  def apply(client: DocumentClient, collectionLink: String): PartitionMetadata = {
    val collectionRoutingMap = getCollectionRoutingMap(client, collectionLink)
    val partitionKeyRangeIds = getCollectionPartitionKeyRangeIds(collectionRoutingMap)
    val partitionKeyDefinition = (client.readCollection(collectionLink, new RequestOptions).getResource).getPartitionKey
    new PartitionMetadata(collectionRoutingMap, partitionKeyRangeIds, partitionKeyDefinition, collectionLink)
  }

  private def getCollectionPartitionKeyRangeIds(collectionRoutingMap: CollectionRoutingMap[Boolean]): List[String] = {
    val fullRange: Range[String] = new Range(PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey,
      PartitionKeyInternal.MaximumExclusiveEffectivePartitionKey,
      true, false)
    collectionRoutingMap.getOverlappingRanges(fullRange).map(_.getId).toList
  }

  private def getCollectionRoutingMap(client: DocumentClient,
                                      collectionLink: String): CollectionRoutingMap[Boolean] = {

    val partitionKeyRanges = client
      .readPartitionKeyRanges(collectionLink, null.asInstanceOf[FeedOptions])
      .getQueryIterable.toList.toList

    val ranges: List[ImmutablePair[PartitionKeyRange, Boolean]] =
      partitionKeyRanges.map(range => new ImmutablePair[PartitionKeyRange, Boolean](range, true))

    val routingMap = InMemoryCollectionRoutingMap.tryCreateCompleteRoutingMap(ranges, "")
    if (routingMap == null) throw new IllegalStateException("Cannot create complete routing map.")
    else routingMap
  }
}
