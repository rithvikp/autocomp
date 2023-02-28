import sbtcrossproject.CrossPlugin.autoImport.CrossType
import sbtcrossproject.CrossPlugin.autoImport.crossProject

name := "frankenpaxos"

lazy val Benchmark = config("bench") extend Test

lazy val frankenpaxos = crossProject(JSPlatform, JVMPlatform)
  .in(file("."))
  .settings(
    name := "frankenpaxos",
    scalacOptions ++= Seq(
      // This option is needed to get nice Java flame graphs. See [1] for more
      // information.
      //
      // [1]: https://medium.com/netflix-techblog/java-in-flames-e763b3d32166
      "-J-XX:+PreserveFramePointer",
      // These flags enable all warnings and make them fatal.
      "-unchecked",
      "-deprecation",
      "-feature",
      "-Xfatal-warnings",
      "-language:postfixOps"
    ),
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % "3.7.0",
      "com.github.tototoshi" %% "scala-csv" % "1.3.5",
      "com.storm-enroute" %% "scalameter" % "0.18",
      "com.thesamet.scalapb" %%% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %%% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
      "io.netty" % "netty-all" % "4.1.34.Final",
      "io.prometheus" % "simpleclient" % "0.6.0",
      "io.prometheus" % "simpleclient_hotspot" % "0.6.0",
      "io.prometheus" % "simpleclient_httpserver" % "0.6.0",
      "org.jgrapht" % "jgrapht-core" % "1.1.0",
      "org.scala-graph" %% "graph-core" % "1.12.5",
      "org.scala-graph" %%% "graph-core" % "1.12.5",
      "org.scala-js" %% "scalajs-library" % scalaJSVersion % "provided",
      "org.scala-js" %% "scalajs-stubs" % scalaJSVersion % "provided",
      "org.scalacheck" %% "scalacheck" % "1.14.0",
      "org.scalactic" %% "scalactic" % "3.0.5",
      "org.scalatest" %% "scalatest" % "3.0.5"
    ),
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ),
    PB.protoSources in Compile := Seq(
      file("shared/src/main/scala"),
      file("jvm/src/main/scala")
    ),
    // These settings enable scalameter. See [1].
    //
    // [1]: https://github.com/scalameter/scalameter-examples/blob/master/basic-with-separate-config/build.sbt
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    parallelExecution in Benchmark := false,
    logBuffered in Benchmark := false
  )
  .jsSettings(
    libraryDependencies += "org.scala-js" %%% "scalajs-java-time" % "0.2.5"
  ).jvmSettings(
    scalaVersion := "2.12.17",
  )
  // These settings enable scalameter. See [1].
  //
  // [1]: https://github.com/scalameter/scalameter-examples/blob/master/basic-with-separate-config/build.sbt
  .configs(Benchmark)
  .settings(
    inConfig(Benchmark)(Defaults.testSettings): _*
  )

lazy val frankenpaxosJVM = frankenpaxos.jvm
lazy val frankenpaxosJS = frankenpaxos.js
