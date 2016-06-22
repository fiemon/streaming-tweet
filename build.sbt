name := "spark-streaming-twitter"

version := "0.0.1"

scalaVersion := "2.11.7"

organization := "org.littlewings"

scalacOptions ++= Seq("-Xlint", "-deprecation", "-unchecked", "-feature")

updateOptions := updateOptions.value.withCachedResolution(true)

resolvers += "CodeLibs Repository" at "http://maven.codelibs.org/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.4.1" % "provided",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.4.1" exclude("org.spark-project.spark", "unused"),
  "org.apache.lucene" % "lucene-analyzers-common" % "5.2.1",
//  "org.apache.lucene" % "lucene-analyzers-kuromoji" % "5.2.1"
  "org.codelibs" % "lucene-analyzers-kuromoji-ipadic-neologd" % "5.3.1-20151231"
)