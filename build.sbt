name := "Secureworks_scala_coding_challenge"

version := "0.1"
mainClass := Some("find_visitors_pipeline")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

scalaVersion := "2.12.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies ++= Seq(("org.apache.spark" %% "spark-core" % "2.4.0").
  exclude("org.mortbay.jetty", "servlet-api").
  exclude("commons-beanutils", "commons-beanutils-core").
  exclude("commons-collections", "commons-collections").
  exclude("commons-logging", "commons-logging").
  exclude("com.esotericsoftware.minlog", "minlog"))
libraryDependencies += "org.scalatest" % "scalatest_2.12" % "3.0.0" % "test"
