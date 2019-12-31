val Monitor = config("monitor") extend Compile
val Daemon = config("daemon") extend Compile

javaOptions in run += "-Xms4G"
javaOptions in run += "-Xmx4G"

// scalaVersion := "2.11.8"
scalaVersion := "2.12.4"

val root = project.in(file(".")).
  configs(Monitor, Daemon).
  settings(
    name := "MapReduce",
    version := "1.0",
    scalacOptions += "-deprecation",
    scalacOptions += "-feature",
    scalacOptions += "-Yrepl-sync",
    fork := true,
    cancelable in Global := true,
    resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    mainClass in (Compile, run) := Some("akka.Main"),
    // libraryDependencies ++= Seq(
    //   "com.typesafe.akka" %% "akka-actor" % "2.4.6",
    //   "com.typesafe.akka" %% "akka-remote" % "2.4.6"
    // )
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.5.11",
      "com.typesafe.akka" %% "akka-remote" % "2.5.11"
    )
    // libraryDependencies ++= Seq(
    //   "com.typesafe.akka" %% "akka-actor" % "2.4.6",
    //   "com.typesafe.akka" %% "akka-remote" % "2.4.6"
    // )
  ).
  settings(inConfig(Monitor)(Classpaths.configSettings ++ Defaults.configTasks ++ baseAssemblySettings ++ Seq(
    assemblyJarName in assembly := "monitor.jar",
    mainClass in assembly := Some("akka.Main")
  )): _*).
  settings(inConfig(Daemon)(Classpaths.configSettings ++ Defaults.configTasks ++ baseAssemblySettings ++ Seq(
    assemblyJarName in assembly := "daemon.jar",
    mainClass in assembly := Some("utils.remote.RemoteMain")
  )): _*)
