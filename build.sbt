name := "spark-json-schema"

version in ThisBuild := "0.4.1"
organization := "org.zalando"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2" % Provided
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.4.10"
dependencyOverrides ++= Set("com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

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
      <developer>
        <name>Adam Dec</name>
        <email>adamdec85@gmail.com</email>
        <url>https://github.com/adamdec</url>
      </developer>
    </developers>
  )