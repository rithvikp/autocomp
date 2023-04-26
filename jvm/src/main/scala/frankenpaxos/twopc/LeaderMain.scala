package frankenpaxos.twopc

import frankenpaxos.Actor
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.LogLevel
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import java.net.InetAddress
import java.net.InetSocketAddress
import java.io.File
import com.github.tototoshi.csv.CSVWriter

object LeaderMain extends App {
  case class Flags(
      configFile: File = new File("."),
      logLevel: LogLevel = frankenpaxos.LogDebug,
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[File]("config").required().action((x, f) => f.copy(configFile = x))
    opt[LogLevel]("log_level").action((x, f) => f.copy(logLevel = x))
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text("-1 to disable")
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  val writer = CSVWriter.open(new java.io.File(".", "leader_log.csv"))
  writer.writeRow(Seq("address", "id", "payload"))

  // Start the leader.
  val logger = new PrintLogger(flags.logLevel)
  val config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath())
  val leader = new Leader[NettyTcpTransport](
    address = config.leaderAddress,
    transport = new NettyTcpTransport(logger),
    logger = logger,
    writer = writer,
    config = config
  )

  // Start Prometheus.
  PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)
}
