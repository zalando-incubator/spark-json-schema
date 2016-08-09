addSbtPlugin("com.github.gseitz" %% "sbt-release" % "1.0.3" withSources())
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

resolvers ++= Seq(Resolver.sonatypeRepo("public"), Resolver.typesafeRepo("releases"))