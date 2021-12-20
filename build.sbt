name := "ExternalDataOTPlugin"

version := "2.0.0"

scalaVersion := "2.12.10"

resolvers += Resolver.jcenterRepo

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.12" % "2.0.0"  % Compile

Test /parallelExecution := false
