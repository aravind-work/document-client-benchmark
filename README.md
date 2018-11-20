
# JMH benchmarks

#### Run on any host by building a fatJar


Update src/main/resources/application.conf

```
    api = "https://your-db-name.documents.azure.com:443/"
    master-key = "INSERT-MASTER_KEY_HERE"
``` 
    
```
sbt clean compile
sbt jmh:compile
sbt jmh:assembly

# copy jar to target environment

java -cp /path/to/target/document-client-benchmark-assembly-0.0.11-SNAPSHOT.jar  org.openjdk.jmh.Main -i 5 -wi 3 -f 1 -t 1 -bm sample BatchQueryBenchmark.batchQueryBenchmark

java -cp /path/to/target/document-client-benchmark-assembly-0.0.11-SNAPSHOT.jar  org.openjdk.jmh.Main -i 5 -wi 3 -f 1 -t 1 -bm sample  -p executorThreads=6 -p partitionBatchSize=6 -p endPartition=5 BatchQueryBenchmark.batchQueryBenchmark

#
# i         -> iterations
# wi        -> warm-up iterations
# f         -> forks
# t         -> thread-count
# bm        -> benchmarking mode (sample/thrpt)
# pattern   -> regex to pick benchmark classes/methods

# for more details run on params
java -cp /path/to/target/document-client-benchmark-assembly-0.0.11-SNAPSHOT.jar  org.openjdk.jmh.Main -h

# refer to results.xlsx spreadsheet results based on a F32 azure VM hitting a cosmosDB provisioned with 1.6M RUs (166 partitions)

```


SBT plugin -- <https://github.com/ktoso/sbt-jmh>
