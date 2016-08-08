addSbtPlugin("com.github.gseitz" %% "sbt-release" % "1.0.3" withSources())

resolvers ++= Seq(Resolver.sonatypeRepo("public"), Resolver.typesafeRepo("releases"))