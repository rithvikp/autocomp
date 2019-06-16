package frankenpaxos.epaxos

import com.github.tototoshi.csv.CSVWriter
import frankenpaxos.Actor
import frankenpaxos.BenchmarkUtil
import frankenpaxos.FileLogger
import frankenpaxos.Flags.durationRead
import frankenpaxos.LogLevel
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import frankenpaxos.PrometheusUtil
import frankenpaxos.monitoring.PrometheusCollectors
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Random

object BenchmarkClientMain extends App {
  case class Flags(
      // Basic flags.
      host: String = "localhost",
      port: Int = 9000,
      configFile: File = new File("."),
      logLevel: frankenpaxos.LogLevel = frankenpaxos.LogDebug,
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      // Benchmark flags.
      duration: java.time.Duration = java.time.Duration.ofSeconds(5),
      timeout: Duration = 10 seconds,
      numClients: Int = 1,
      numKeys: Int = 1,
      outputFilePrefix: String = "",
      // Options.
      options: ClientOptions = ClientOptions.default
  )

  implicit class OptionsWrapper[A](o: scopt.OptionDef[A, Flags]) {
    def optionAction(
        f: (A, ClientOptions) => ClientOptions
    ): scopt.OptionDef[A, Flags] =
      o.action((x, flags) => flags.copy(options = f(x, flags.options)))
  }

  val parser = new scopt.OptionParser[Flags]("") {
    // Basic flags.
    opt[String]("host").required().action((x, f) => f.copy(host = x))
    opt[Int]("port").required().action((x, f) => f.copy(port = x))
    opt[File]("config").required().action((x, f) => f.copy(configFile = x))
    opt[LogLevel]("log_level").required().action((x, f) => f.copy(logLevel = x))

    // Monitoring.
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text(s"Prometheus port; -1 to disable")

    // Benchmark flags.
    opt[java.time.Duration]("duration")
      .action((x, f) => f.copy(duration = x))
    opt[Duration]("timeout")
      .action((x, f) => f.copy(timeout = x))
    opt[Int]("num_clients")
      .action((x, f) => f.copy(numClients = x))
    opt[Int]("num_keys")
      .action((x, f) => f.copy(numKeys = x))
    opt[String]("output_file_prefix")
      .action((x, f) => f.copy(outputFilePrefix = x))

    // Options.
    opt[java.time.Duration]("options.reproposePeriod")
      .optionAction((x, o) => o.copy(reproposePeriod = x))
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Start prometheus.
  val prometheusServer =
    PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)

  // Construct client.
  val logger = new PrintLogger(flags.logLevel)
  val transport = new NettyTcpTransport(logger);
  val client = new Client[NettyTcpTransport](
    address = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port)),
    transport = transport,
    logger = logger,
    config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath()),
    options = flags.options,
    metrics = new ClientMetrics(PrometheusCollectors)
  )

  // Helper function to generate command.
  def randomProposal(): KeyValueStoreInput = {
    val key = Random.nextInt(flags.numKeys).toString()
    val isGet = Random.nextBoolean()
    if (isGet) {
      KeyValueStoreInput().withGetRequest(GetRequest(key = Seq(key)))
    } else {
      KeyValueStoreInput().withSetRequest(
        SetRequest(
          keyValue = Seq(SetKeyValuePair(key = key, value = "doesnt_matter"))
        )
      )
    }
  }

  // Run clients.
  val latencyWriter =
    CSVWriter.open(new File(s"${flags.outputFilePrefix}_data.csv"))
  latencyWriter.writeRow(Seq("start", "stop", "latency_nanos", "host", "port"))

  def run(pseudonym: Int): Future[Unit] = {
    implicit val context = transport.executionContext
    BenchmarkUtil
      .timed(() => client.propose(pseudonym, randomProposal().toByteArray))
      .transformWith({
        case scala.util.Failure(_) =>
          logger.debug("EPaxos request failed.")
          Future.successful(())

        case scala.util.Success((_, timing)) =>
          latencyWriter.writeRow(
            Seq(timing.startTime.toString(),
                timing.stopTime.toString(),
                timing.durationNanos.toString(),
                flags.host,
                flags.port)
          )
          Future.successful(())
      })
  }

  implicit val context = transport.executionContext
  val futures = for (pseudonym <- 0 to flags.numClients)
    yield BenchmarkUtil.runFor(() => run(pseudonym), flags.duration)

  // Wait for the benchmark to finish.
  concurrent.Await.result(Future.sequence(futures), flags.timeout)

  // Shut everything down.
  logger.debug("Shutting down transport.")
  transport.shutdown()
  logger.debug("Transport shut down.")

  prometheusServer.foreach(server => {
    logger.info("Stopping prometheus.")
    server.stop()
    logger.info("Prometheus stopped.")
  })
}
