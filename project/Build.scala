import sbt._
import sbt.Keys._
import spray.revolver.RevolverPlugin._

import scala.util.Properties

// sbt-assembly
import sbtassembly.AssemblyPlugin.autoImport._

// Revolver
import spray.revolver.RevolverPlugin._

object Version {
  val foundry = "0.1.0"
  val scala = "2.11.7"
  val spark = "1.4.0"
  val hadoop = "2.5.0"
  val geotrellis = "0.10.0-SNAPSHOT"
  val spray = "1.3.2"
}

object Build extends Build {
  override lazy val settings =
    super.settings ++
  Seq(
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
    version := Version.foundry,
    scalaVersion := Version.scala,
    organization := "com.azavea",

    scalacOptions ++=
      Seq("-deprecation",
        "-unchecked",
        "-Yinline-warnings",
        "-language:implicitConversions",
        "-language:reflectiveCalls",
        "-language:higherKinds",
        "-language:postfixOps",
        "-language:existentials",
        "-feature"),
    resolvers ++= Seq(
    "sonatypeSnapshots"       at "https://oss.sonatype.org/content/repositories/snapshots"
    ),
    test in assembly := {}
  )
 
  // Project: root
  lazy val root =
    Project("foundry", file("."))
      .aggregate(etl, server)
  
  lazy val etl = Project(
    id = "etl",
    base = file("etl"),
    settings = Project.defaultSettings ++ Seq(
      name := "foundry-etl",
      organization := "com.azavea",
      version := "0.1.0-SNAPSHOT",
      fork in run := true,
      libraryDependencies ++= {
        val geotrellisV = "0.10.0-M1"

        Seq(
          "org.apache.spark" %% "spark-core" % Version.spark,
          "org.apache.hadoop" % "hadoop-client" % Version.hadoop,
          "com.azavea.geotrellis" %% "geotrellis-spark"   % Version.geotrellis,

          "org.apache.commons" % "commons-io" % "1.3.2",
 
          "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test"
        )
      },

      assemblyMergeStrategy in assembly := { 
        case x if Assembly.isConfigFile(x) =>
          MergeStrategy.concat
        case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
          MergeStrategy.rename
        case PathList("META-INF", xs @ _*) =>
          (xs map {_.toLowerCase}) match {
            case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
              MergeStrategy.discard
            case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
              MergeStrategy.discard
            case "plexus" :: xs =>
              MergeStrategy.discard
            case "services" :: xs =>
              MergeStrategy.filterDistinctLines
            case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
              MergeStrategy.filterDistinctLines
            case _ => MergeStrategy.deduplicate
          }
        case _ => MergeStrategy.first
      }
    ) ++ net.virtualvoid.sbt.graph.Plugin.graphSettings
  )

  lazy val server = Project(
    id = "server",
    base = file("server"),
    settings = Project.defaultSettings ++ Seq(
      name := "foundry-server",
      organization := "com.azavea",
      version := "0.1.0-SNAPSHOT",
      fork in run := true,
      libraryDependencies ++= {
        val geotrellisV = "0.10.0-M1"

        Seq(
          "org.apache.spark" %% "spark-core" % Version.spark,
          "com.azavea.geotrellis" %% "geotrellis-spark"   % Version.geotrellis,
          "org.apache.commons" % "commons-io" % "1.3.2",
          "com.amazonaws" % "aws-java-sdk-s3" % "1.9.34",

          "io.spray"            %%   "spray-can"     % Version.spray,
          "io.spray"            %%   "spray-routing" % Version.spray,
          "io.spray"            %%   "spray-json"    % "1.3.1",
 
          "org.scalatest" %% "scalatest" % "2.2.5" % "test"
        )
      },
      assemblyMergeStrategy in assembly := { 
        case x if Assembly.isConfigFile(x) =>
          MergeStrategy.concat
        case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
          MergeStrategy.rename
        case PathList("META-INF", xs @ _*) =>
          (xs map {_.toLowerCase}) match {
            case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
              MergeStrategy.discard
            case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
              MergeStrategy.discard
            case "plexus" :: xs =>
              MergeStrategy.discard
            case "services" :: xs =>
              MergeStrategy.filterDistinctLines
            case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
              MergeStrategy.filterDistinctLines
            case _ => MergeStrategy.deduplicate
          }
        case _ => MergeStrategy.first
      }
    ) ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++ Revolver.settings
  )

}
