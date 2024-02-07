package frankenpaxos.multipaxos

import frankenpaxos.Actor
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.statemachine
import frankenpaxos.statemachine.StateMachine
import frankenpaxos.statemachine.StateMachine.read
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.duration

object ReplicaMain extends App {
  case class Flags(
      // Basic flags.
      index: Int = -1,
      configFile: File = new File("."),
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      stateMachine: StateMachine = new statemachine.Noop(),
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Options.
      options: ReplicaOptions = ReplicaOptions.default,
      receiveAddrs: String = ""
  )

  implicit class OptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def optionAction(
        f: (A, ReplicaOptions) => ReplicaOptions
    ): scopt.OptionDef[A, Flags] =
      o.action((x, flags) => flags.copy(options = f(x, flags.options)))
  }

  val parser = new scopt.OptionParser[Flags]("") {
    help("help")

    // Basic flags.
    opt[Int]("index").required().action((x, f) => f.copy(index = x))
    opt[File]("config").required().action((x, f) => f.copy(configFile = x))
    opt[LogLevel]("log_level").required().action((x, f) => f.copy(logLevel = x))
    opt[StateMachine]("state_machine")
      .required()
      .action((x, f) => f.copy(stateMachine = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"-1 to disable")

    opt[String]("receive_addrs").action((x, f) => f.copy(receiveAddrs = x))

    // Options.
    opt[Int]("options.logGrowSize")
      .optionAction((x, o) => o.copy(logGrowSize = x))
    opt[Boolean]("options.unsafeDontUseClientTable")
      .optionAction((x, o) => o.copy(unsafeDontUseClientTable = x))
    opt[Int]("options.sendChosenWatermarkEveryNEntries")
      .optionAction((x, o) => o.copy(sendChosenWatermarkEveryNEntries = x))
    opt[java.time.Duration]("options.recoverLogEntryMinPeriod")
      .optionAction((x, o) => o.copy(recoverLogEntryMinPeriod = x))
    opt[java.time.Duration]("options.recoverLogEntryMaxPeriod")
      .optionAction((x, o) => o.copy(recoverLogEntryMaxPeriod = x))
    opt[Boolean]("options.unsafeDontRecover")
      .optionAction((x, o) => o.copy(unsafeDontRecover = x))
    opt[Boolean]("options.connectToLeader")
      .optionAction((x, o) => o.copy(connectToLeader = x))
    opt[Boolean]("options.bft")
      .optionAction((x, o) => o.copy(bft = x))
  }

  // Parse flags.
  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Construct replica.
  val logger = new PrintLogger(flags.logLevel)
  val config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath())
  val replica = new Replica[NettyTcpTransport](
    address = config.replicaAddresses(flags.index),
    transport = new NettyTcpTransport(logger),
    logger = logger,
    stateMachine = flags.stateMachine,
    config = config,
    options = flags.options
  )

  val receiveChans: Unit = for (addr <- flags.receiveAddrs.split(",")) {
    if (addr != "") {
      addr.split(":") match {
        case Array(host, port) =>
          replica.send(NettyTcpAddress(new InetSocketAddress(host, port.toInt)), Array[Byte]())
        case default => throw new IllegalArgumentException(s"Invalid receive address $addr")
      }
    }
  }

  // Start Prometheus.
  PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)
}
