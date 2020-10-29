name := "HBase_Example"

version := "0.1"

scalaVersion := "2.13.3"

resolvers += "Apache HBase" at "https://repository.apache.org/content/repositories/releases"

resolvers += "Thrift" at "https://people.apache.org/~rawson/repo/"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.apache.hbase" % "hbase" % "2.3.2",
  "org.apache.hbase" % "hbase-client" % "2.3.2"
)
