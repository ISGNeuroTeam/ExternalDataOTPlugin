name := "ExternalDataOTPlugin"

version := "0.0.1_feature_SQLRead"

scalaVersion := "2.11.12"

resolvers += Resolver.jcenterRepo

libraryDependencies += "dispatcher" % "dispatcher_2.11" % "1.1.0"  % Compile

libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "8.2.0.jre8"

libraryDependencies += "ojdbc" % "ojdbc" % "14"
