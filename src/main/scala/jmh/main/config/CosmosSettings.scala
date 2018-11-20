package jmh.main.config

import com.microsoft.azure.documentdb.{ConnectionMode, ConsistencyLevel}
import com.typesafe.config.Config
case class CosmosSettings(api: String,
                          masterKey: String,
                          dbName: String,
                          tablePrefix: String,
                          tableName: String,
                          maxPageSize: Int = CosmosSettings.DEFAULT_COSMOS_PAGE_SIZE,
                          maxPoolSize: Int = CosmosSettings.DEFAULT_COSMOS_POOL_SIZE,
                          connectionMode: ConnectionMode,
                          consistencyLevel: ConsistencyLevel = ConsistencyLevel.Strong)

object CosmosSettings {
  val DEFAULT_COSMOS_PAGE_SIZE = -1
  val DEFAULT_COSMOS_POOL_SIZE = 10000

  val PARAM_COSMOS_API = "benchmark.cosmos.api"
  val PARAM_COSMOS_MASTER_KEY = "benchmark.cosmos.master-key"
  val PARAM_COSMOS_DB = "benchmark.cosmos.db-name"
  val PARAM_COSMOS_TABLE_PREFIX = "benchmark.cosmos.table-prefix"
  val PARAM_COSMOS_TABLE_NAME = "benchmark.cosmos.table-name"
  val PARAM_COSMOS_PAGE_SIZE = "benchmark.cosmos.maxPageSize"
  val PARAM_COSMOS_POOL_SIZE = "benchmark.cosmos.maxPoolSize"
  val PARAM_COSMOS_CONNECTION_MODE = "benchmark.cosmos.connection"
  val PARAM_COSMOS_CONSISTENCY_LEVEL_MODE = "benchmark.cosmos.consistencyLevel"


  def apply(config: Config): CosmosSettings = {
    val cosmosApi = config.getString(PARAM_COSMOS_API)
    val cosmosMasterKey = config.getString(PARAM_COSMOS_MASTER_KEY)
    val cosmosDBName = config.getString(PARAM_COSMOS_DB)
    val cosmosTablePrefix = config.getString(PARAM_COSMOS_TABLE_PREFIX)
    val cosmosTableName = config.getString(PARAM_COSMOS_TABLE_NAME)


    val cosmosMaxPageSize = config.getIntOrElse(PARAM_COSMOS_PAGE_SIZE, DEFAULT_COSMOS_PAGE_SIZE)
    val cosmosMaxPoolSize = config.getIntOrElse(PARAM_COSMOS_POOL_SIZE, DEFAULT_COSMOS_POOL_SIZE)
    val cosmosConnectionMode = config.getOptionalString(PARAM_COSMOS_CONNECTION_MODE) match {
      case None => ConnectionMode.DirectHttps
      case Some(someMode) => getCosmosConnectionMode(someMode)
    }

    val cosmosConsistencyLevel = config.getOptionalString(PARAM_COSMOS_CONSISTENCY_LEVEL_MODE) match {
      case None => ConsistencyLevel.Strong
      case Some(someLevel) => getCosmosConsistencyLevel(someLevel)
    }

    CosmosSettings(
      cosmosApi,
      cosmosMasterKey,
      cosmosDBName,
      cosmosTablePrefix,
      cosmosTableName,
      cosmosMaxPageSize,
      cosmosMaxPoolSize,
      cosmosConnectionMode,
      cosmosConsistencyLevel
    )
  }

  def getCosmosConnectionMode(mode: String): ConnectionMode = {
    mode match {
      case d if d.toLowerCase == ConnectionMode.DirectHttps.toString.toLowerCase => ConnectionMode.DirectHttps
      case g if g.toLowerCase == ConnectionMode.Gateway.toString.toLowerCase => ConnectionMode.Gateway
      case _ => ConnectionMode.DirectHttps
    }
  }

  // scalastyle:off cyclomatic.complexity
  def getCosmosConsistencyLevel(level: String): ConsistencyLevel = {
    level match {
      case l if l.toLowerCase == ConsistencyLevel.Session.toString.toLowerCase => ConsistencyLevel.Session
      case l if l.toLowerCase == ConsistencyLevel.Strong.toString.toLowerCase => ConsistencyLevel.Strong
      case l if l.toLowerCase == ConsistencyLevel.Eventual.toString.toLowerCase => ConsistencyLevel.Eventual
      case l if l.toLowerCase == ConsistencyLevel.BoundedStaleness.toString.toLowerCase => ConsistencyLevel.BoundedStaleness
      case l if l.toLowerCase == ConsistencyLevel.ConsistentPrefix.toString.toLowerCase => ConsistencyLevel.ConsistentPrefix
      case _ => ConsistencyLevel.Strong
    }
  }
}