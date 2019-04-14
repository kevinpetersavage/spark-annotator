name := "spark-annotator"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1" % "provided"

libraryDependencies += "org.opencb.biodata" % "biodata-tools" % "1.4.3" excludeAll(
  ExclusionRule(organization = "com.fasterxml.jackson.core"),
  ExclusionRule(organization = "BigWig")
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}