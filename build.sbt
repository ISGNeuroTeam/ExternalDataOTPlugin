name := "ExternalDataOTPlugin"

version := "1.0.1"

scalaVersion := "2.11.12"

resolvers += Resolver.jcenterRepo

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.1.0"  % Compile
