organization := "com.jvstinian"
name := "plttools-spark"
version := "0.1.0"
scalaVersion := "2.11.12" // Scala version needs to match version of Scaala used by Spark

initialize := {
  val _ = initialize.value
  val javaVersion = sys.props("java.specification.version")
  if (javaVersion != "1.8")
    sys.error("Java 8 is required for this project. Found " + javaVersion + " instead")
}

inThisBuild(
  List(
    scalaVersion := "2.11.12", // Needs to match line 4
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    scalacOptions ++= List(
      "-Xlint",
      "-Ywarn-unused",
      "-Ywarn-unused-import",
      "-Ywarn-dead-code",
      "-Ywarn-adapted-args",
      "-deprecation"
    ),
    scalafixScalaBinaryVersion := scalaBinaryVersion.value,
    scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.5.0"
  )
)

mainClass in (Compile, run) := Some("main.scala.RMSPLTAnalyticsExample")

// Assembly options
assemblyOption in assembly ~= {
  _.withIncludeScala(false)
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Spark unit test settings
fork in Test := true
parallelExecution in Test := false

// Test dependencies
val scalaTestVersion = "3.2.9"
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % scalaTestVersion % "test"

// Spark dependencies
val sparkVersion = "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion 
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion 
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
