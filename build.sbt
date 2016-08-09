name := "spark-json-schema"

version in ThisBuild := "0.1-SNAPSHOT"
organization := "de.zalando.payana"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.5.4"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

publishMavenStyle := true
scalacOptions += "-deprecation"

