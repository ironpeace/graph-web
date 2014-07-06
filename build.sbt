name := "call-spark-sample"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  jdbc,
  anorm,
  cache,
  "org.apache.spark" %% "spark-core" % "0.9.1",
  "org.apache.spark" %% "spark-graphx" % "0.9.1",
  "org.apache.spark" %% "spark-bagel" % "0.9.1",
  "com.typesafe.akka" %% "akka-actor" % "2.2.3",
  "com.typesafe.akka" %% "akka-slf4j" % "2.2.3",
  "com.github.aselab" %% "scala-activerecord" % "0.2.3",
  "org.slf4j" % "slf4j-nop" % "1.7.5",
  "com.h2database" % "h2" % "1.3.173"
)

play.Project.playScalaSettings
