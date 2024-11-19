name := "dsql"
version := "0.1.0"
scalaVersion := "3.5.2"

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jbdc" % "3.47.0.0",
  "com.zaxxer" % "HikariCP" % "6.2.1"
)
