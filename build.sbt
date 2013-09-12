// Demonstrate tests that don't seem to fork.
//   sbt test     // fails
//   sbt "testOnly com.spotright.polidoro.TestGet" "testOnly com.spotright.polidoro.TestEg"    // succeeds

name := "Polidoro"

organization := "com.spotright"

version := "1.0.4"

scalaVersion := "2.10.2"

libraryDependencies ++= {
  val astyVer = "1.56.42"
  val cassVer = "1.2.3"
  val slf4jVer = "1.6.6"
  Seq(
    "com.netflix.astyanax" % "astyanax-cassandra" % astyVer exclude("org.apache.cassandra", "cassandra-all"),
    "com.netflix.astyanax" % "astyanax-thrift" % astyVer exclude("org.apache.cassandra", "cassandra-all") exclude("org.apache.cassandra", "cassandra-thrift"),
    "commons-io" % "commons-io" % "2.0",
    "org.apache.cassandra" % "cassandra-all" % cassVer,
    "org.scalaz" %% "scalaz-core" % "7.0.3",
    "log4j" % "log4j" % "1.2.17",
    "org.slf4j" % "slf4j-api" % slf4jVer force(),
    "org.slf4j" % "slf4j-log4j12" % slf4jVer % "test" force(),
    "org.specs2" %% "specs2" % "2.1.1" % "test",
    "org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
    "junit" % "junit" % "4.10" % "test"
  )
}

// force() on slf4j didn't seem to work above
dependencyOverrides := {
  val slf4jVer = "1.6.6"
  Set(
    "org.slf4j" % "slf4j-api" % slf4jVer,
    "org.slf4j" % "slf4j-log4j12" % slf4jVer % "test"
  )
}

parallelExecution in Test := false

parallelExecution in Global := false

TestPolicy.forkedUnique

net.virtualvoid.sbt.graph.Plugin.graphSettings
