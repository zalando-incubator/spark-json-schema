name := "spark-json-schema"

version in ThisBuild := "0.6.4"
organization := "org.zalando"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"  % Provided
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.1"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.2"
dependencyOverrides ++= Set("com.fasterxml.jackson.core" % "jackson-databind" % "2.11.2")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

scapegoatVersion := "1.3.0"
scapegoatIgnoredFiles := Seq(s"${target.value}.*.scala")

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

//pom extra info
publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomExtra := (
  <scm>
    <url>git@github.com:zalando-incubator/spark-json-schema.git</url>
    <developerConnection>scm:git:git@github.com:zalando-incubator/spark-json-schema.git</developerConnection>
    <connection>scm:git:https://github.com/zalando-incubator/spark-json-schema.git</connection>
  </scm>
  <developers>
    <developer>
      <name>Henning-Ulrich Esser</name>
      <email>henning-ulrich.esser@zalando.de</email>
      <url>https://github.com/zalando</url>
    </developer>
    <developer>
      <name>Patrick Baier</name>
      <email>patrick.baier@zalando.de</email>
      <url>https://github.com/zalando</url>
    </developer>
  </developers>
)
