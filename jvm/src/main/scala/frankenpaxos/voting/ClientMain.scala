package frankenpaxos.voting

import collection.mutable
import com.github.tototoshi.csv.CSVWriter
import frankenpaxos.Actor
import frankenpaxos.BenchmarkUtil
import frankenpaxos.FileLogger
import frankenpaxos.Flags.durationRead
import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import frankenpaxos.PrintLogger
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Try
import java.io.File
import frankenpaxos.LogLevel

object ClientMain extends App {
  case class Flags(
      host: String = "localhost",
      port: Int = 9001,
      configFile: File = new File("."),
      logLevel: LogLevel = frankenpaxos.LogDebug,
      duration: java.time.Duration = java.time.Duration.ofSeconds(0),
      timeout: Duration = 0 seconds,
      numClients: Int = 1,
      outputFile: String = "",
      warmupDuration: java.time.Duration = java.time.Duration.ofSeconds(0),
      warmupTimeout: Duration = 0 seconds,
      warmupSleep: java.time.Duration = java.time.Duration.ofSeconds(0),
      numWarmupClients: Int = 1,
      outputFilePrefix: String = "",
      // Monitoring.
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[String]("host").action((x, f) => f.copy(host = x))
    opt[Int]("port").action((x, f) => f.copy(port = x))
    opt[LogLevel]("log_level").required().action((x, f) => f.copy(logLevel = x))
    opt[java.time.Duration]("duration").action((x, f) => f.copy(duration = x))
    opt[Duration]("timeout").action((x, f) => f.copy(timeout = x))
    opt[Int]("num_clients").action((x, f) => f.copy(numClients = x))
    opt[String]("output_file").action((x, f) => f.copy(outputFile = x))
    opt[java.time.Duration]("warmup_duration").action((x, f) => f.copy(warmupDuration = x))
    opt[Duration]("warmup_timeout").required().action((x, f) => f.copy(warmupTimeout = x))
    opt[java.time.Duration]("warmup_sleep").action((x, f) => f.copy(warmupSleep = x))
    opt[Int]("num_warmup_clients").action((x, f) => f.copy(numWarmupClients = x))
    opt[String]("outputFilePrefix").action((x, f) => f.copy(outputFilePrefix = x))
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

  // Start the client.
  val logger = new PrintLogger()
  val transport = new NettyTcpTransport(logger)
  val config = ConfigUtil.fromFile(flags.configFile.getAbsolutePath())
  val client = new Client[NettyTcpTransport](
    address = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port)),
    config = config,
    transport = transport,
    logger = logger
  )

  // Run clients.
  val recorder =
    new BenchmarkUtil.Recorder(flags.outputFile)

  def warmupRun(): Future[Unit] = {
    implicit val context = transport.executionContext
    client
      .request()
      .transformWith({
        case scala.util.Failure(_) =>
          logger.debug("Request failed.")
          Future.successful(())

        case scala.util.Success(_) =>
          Future.successful(())
      })
  }

  def run(): Future[Unit] = {
    implicit val context = transport.executionContext
    BenchmarkUtil
      .timed(() => client.request())
      .transformWith({
        case scala.util.Failure(_) =>
          logger.debug("Request failed.")
          Future.successful(())

        case scala.util.Success((_, timing)) =>
          recorder.record(
            start = timing.startTime,
            stop = timing.stopTime,
            latencyNanos = timing.durationNanos,
            host = flags.host,
            port = flags.port
          )
          Future.successful(())
      })
  }

  // Warm up the protocol
  implicit val context = transport.executionContext
  val warmupFutures =
    for (_ <- 0 to flags.numWarmupClients)
      yield BenchmarkUtil.runFor(() => warmupRun(), flags.warmupDuration)
  try {
    logger.info("Client warmup started.")
    concurrent.Await.result(Future.sequence(warmupFutures), flags.warmupTimeout)
    logger.info("Client warmup finished successfully.")
  } catch {
    case e: java.util.concurrent.TimeoutException =>
      logger.warn("Client warmup futures timed out!")
      logger.warn(e.toString())
  }

  // Sleep to let protocol settle.
  Thread.sleep(flags.warmupSleep.toMillis())

  // Run the benchmark.
  val futures =
    for (_ <- 0 to flags.numClients)
      yield BenchmarkUtil.runFor(() => run(), flags.duration)
  try {
    logger.info("Clients started.")
    concurrent.Await.result(Future.sequence(futures), flags.timeout)
    logger.info("Clients finished successfully.")
  } catch {
    case e: java.util.concurrent.TimeoutException =>
      logger.warn("Client futures timed out!")
      logger.warn(e.toString())
  }

  // Shut everything down.
  logger.info("Shutting down transport.")
  transport.shutdown()
  logger.info("Transport shut down.")
}