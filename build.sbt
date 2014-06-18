import _root_.sbt.Keys._

name := "akka-dsgd"

version := "1.0"


scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.1.1",
  "org.scalanlp" %% "breeze" % "0.8.1",
  "org.scalanlp" %% "breeze-natives" % "0.8.1"
)


resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"