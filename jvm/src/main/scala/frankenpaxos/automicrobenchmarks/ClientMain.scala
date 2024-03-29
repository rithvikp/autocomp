package frankenpaxos.automicrobenchmarks

import collection.mutable
import com.github.tototoshi.csv.CSVWriter
import frankenpaxos.Actor
import frankenpaxos.BenchmarkUtil
import frankenpaxos.PrometheusUtil
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
import frankenpaxos.multipaxos.ReadWriteWorkload
import frankenpaxos.multipaxos.UniformReadWriteWorkload
import frankenpaxos.multipaxos.Write
import frankenpaxos.multipaxos.Read

object ClientMain extends App {
  case class Flags(
      host: String = "localhost",
      port: Int = 9001,
      leaderHost: String = "localhost",
      leaderPort: Int = 9000,
      logLevel: LogLevel = frankenpaxos.LogDebug,
      duration: java.time.Duration = java.time.Duration.ofSeconds(0),
      timeout: Duration = 0 seconds,
      numClients: Int = 1,
      outputFile: String = "",
      warmupDuration: java.time.Duration = java.time.Duration.ofSeconds(0),
      warmupTimeout: Duration = 0 seconds,
      warmupSleep: java.time.Duration = java.time.Duration.ofSeconds(0),
      numWarmupClients: Int = 1,
      prometheusHost: String = "0.0.0.0",
      prometheusPort: Int = 8009,
      receiveAddrs: String = ""
  )

  val parser = new scopt.OptionParser[Flags]("") {
    opt[String]("host").action((x, f) => f.copy(host = x))
    opt[Int]("port").action((x, f) => f.copy(port = x))
    opt[LogLevel]("log_level").action((x, f) => f.copy(logLevel = x))
    opt[String]("leader_host").action((x, f) => f.copy(leaderHost = x))
    opt[Int]("leader_port").action((x, f) => f.copy(leaderPort = x))
    opt[java.time.Duration]("duration").action((x, f) => f.copy(duration = x))
    opt[Duration]("timeout").action((x, f) => f.copy(timeout = x))
    opt[Int]("num_clients").action((x, f) => f.copy(numClients = x))
    opt[String]("output_file").action((x, f) => f.copy(outputFile = x))
    opt[java.time.Duration]("warmup_duration").action((x, f) => f.copy(warmupDuration = x))
    opt[Duration]("warmup_timeout").required().action((x, f) => f.copy(warmupTimeout = x))
    opt[java.time.Duration]("warmup_sleep").action((x, f) => f.copy(warmupSleep = x))
    opt[Int]("num_warmup_clients").action((x, f) => f.copy(numWarmupClients = x))
    opt[String]("prometheus_host")
      .action((x, f) => f.copy(prometheusHost = x))
    opt[Int]("prometheus_port")
      .action((x, f) => f.copy(prometheusPort = x))
      .text("-1 to disable")
    opt[String]("receive_addrs").action((x, f) => f.copy(receiveAddrs = x))
  }

  val flags: Flags = parser.parse(args, Flags()) match {
    case Some(flags) =>
      flags
    case None =>
      throw new IllegalArgumentException("Could not parse flags.")
  }

  // Start the client.
  val logger = new PrintLogger(flags.logLevel)
  val transport = new NettyTcpTransport(logger)
  val client = new Client[NettyTcpTransport](
    address = NettyTcpAddress(new InetSocketAddress(flags.host, flags.port)),
    dstAddress = NettyTcpAddress(new InetSocketAddress(flags.leaderHost, flags.leaderPort)),
    transport = transport,
    logger = logger
  )

  val receiveChans: Unit = for (addr <- flags.receiveAddrs.split(",")) {
    if (addr != "") {
      addr.split(":") match {
        case Array(host, port) =>
          client.send(NettyTcpAddress(new InetSocketAddress(host, port.toInt)), Array[Byte]())
        case default => throw new IllegalArgumentException(s"Invalid receive address $addr")
      }
    }
  }

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

  val prometheusServer = PrometheusUtil.server(flags.prometheusHost, flags.prometheusPort)

  // Warm up the protocol
  implicit val context = transport.executionContext
  val warmupFutures =
    for (_ <- 0 until flags.numWarmupClients)
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
    for (_ <- 0 until flags.numClients)
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

  prometheusServer.foreach(server => {
    logger.info("Stopping prometheus.")
    server.stop()
    logger.info("Prometheus stopped.")
  })
}
