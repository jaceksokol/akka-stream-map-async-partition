name := "akka-stream-map-async-partition"

ThisBuild / organization := "com.github.jaceksokol"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.6"
ThisBuild / versionScheme := Some("semver-spec")

scalacOptions ++= Seq(
  "-encoding",
  "utf8",
  "-Xfatal-warnings",
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:postfixOps"
)

val AkkaVersion = "2.6.14"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-typed" % AkkaVersion,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % Test
)

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "content/repositories/releases")
}
