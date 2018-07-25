lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.holidaycheck",
      scalaVersion := "2.12.6",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "kafka-talk",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "1.1.0"
    )
  )
