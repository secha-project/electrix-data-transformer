scalaVersion := "2.12.20"

val transformerPath: String = "transformer"
val cleanerPath: String = "cleaner"

lazy val transformer = project
    .in(file(transformerPath))

lazy val cleaner = project
    .in(file(cleanerPath))
