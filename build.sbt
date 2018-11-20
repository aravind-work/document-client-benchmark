name := "document-client-benchmark"
version := "1.0"
scalaVersion := "2.11.11"
logLevel := Level.Warn

enablePlugins(JmhPlugin)

libraryDependencies ++= Seq(
  // document db sdk
  "com.microsoft.azure" % "azure-documentdb" % "2.0.0",
  
  "org.scala-lang" % "scala-reflect" % scalaVersion.value, 
  
  // configuration management
  "com.typesafe" % "config" % "1.3.3",
  
  // use sl4j
  "org.slf4j" % "slf4j-api" % "1.7.6",
  // bind slf4j with log4j for test
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.9.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.9.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.9.1"
)


