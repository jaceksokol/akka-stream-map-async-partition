name := "akka-stream-map-async-partition"

ThisBuild / organization := "com.github.jaceksokol"
ThisBuild / scalaVersion := "2.13.7"
ThisBuild / versionScheme := Some("semver-spec")

publishMavenStyle := true
homepage := Some(url("https://github.com/jaceksokol/akka-stream-map-async-partition"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/jaceksokol/akka-stream-map-async-partition"),
    "scm:git@github.com:jaceksokol/akka-stream-map-async-partition.git"
  )
)
developers := List(
  Developer("jaceksokol", "Jacek Sokół", "jacek@scalabs.pl", url("https://github.com/jaceksokol"))
)
licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT"))

scalacOptions ++= Seq(
  "-encoding",
  "utf8",
  "-Xfatal-warnings",
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:postfixOps"
)

val AkkaVersion = "2.6.17"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-typed" % AkkaVersion,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % Test
)

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

releaseUseGlobalVersion := false
releaseIgnoreUntrackedFiles := true
releasePublishArtifactsAction := PgpKeys.publishSigned.value
publishConfiguration := publishConfiguration.value.withOverwrite(true)

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
