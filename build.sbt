name := "ExternalDataOTPlugin"

version := "1.0.0"

scalaVersion := "2.11.12"

resolvers += (
  "Local Maven Repository".at(s"file:///${Path.userHome.absolutePath}/.m2/repository")
  )

resolvers += Resolver.jcenterRepo

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.1.0"  % Compile

parallelExecution in Test := false
