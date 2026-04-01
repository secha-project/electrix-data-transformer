name := "data-transformer"
version := "1.0.0"
scalaVersion := "2.13.18"
val MainClass: String = "app.DataTransformer"

val SparkVersion: String = "4.1.1"
val SttpVersion: String = "4.0.21"

Compile / run / mainClass := Some(MainClass)
Compile / scalacOptions += "-Xlint"
assembly / mainClass := Some(MainClass)
assembly / assemblyJarName := s"${name.value}-${version.value}.jar"
assembly / assemblyMergeStrategy := {
	case path if path == "module-info.class" => MergeStrategy.discard
	case path if path.startsWith("META-INF/versions/") && path.endsWith("/module-info.class") => MergeStrategy.discard
	case "META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat" => MergeStrategy.discard
	case path => (assembly / assemblyMergeStrategy).value(path)
}

libraryDependencies += "com.softwaremill.sttp.client4" %% "core" % SttpVersion
libraryDependencies += "org.apache.spark" %% "spark-connect-client-jvm" % SparkVersion
