name := "spark-json-schema"

version in ThisBuild := "0.1"
organization := "org.zalando"

scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")
releaseCrossBuild := true

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.5.4"
dependencyOverrides ++= Set("com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

publishMavenStyle := true
scalacOptions += "-deprecation"

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))
