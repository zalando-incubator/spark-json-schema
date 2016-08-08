name := "spark-json-schema"

version in ThisBuild := "0.1-SNAPSHOT"
organization := "de.zalando.payana"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.4.6"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

sbtPlugin := true
publishMavenStyle := true
scalacOptions += "-deprecation"

