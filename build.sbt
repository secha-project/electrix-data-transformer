name := "data-transformer"
version := "1.0.0"
scalaVersion := "2.13.16"

val MainClass: String = "app.DataTransformer"

val DeltaVersion: String = "3.3.2"
val SparkVersion: String = "3.5.6"

Compile / run / mainClass := Some(MainClass)
Compile / scalacOptions += "-Xlint"
assembly / mainClass := Some(MainClass)
assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

libraryDependencies += "io.delta" %% "delta-spark" % DeltaVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % SparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % SparkVersion % "provided"
