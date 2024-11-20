name := "dsql"
version := "0.1.0"
scalaVersion := "3.5.2"

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.47.0.0",
  "com.zaxxer" % "HikariCP" % "5.1.0",
  "org.scalatest" %% "scalatest" % "3.2.15"
)
