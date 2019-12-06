package frankenpaxos.matchmakermultipaxos

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.election.basic.ElectionOptions
import frankenpaxos.election.basic.Participant
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.Gauge
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.roundsystem.RoundSystem
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object MatchmakerInboundSerializer extends ProtoSerializer[MatchmakerInbound] {
  type A = MatchmakerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Matchmaker {
  val serializer = MatchmakerInboundSerializer
}

@JSExportAll
case class MatchmakerOptions(
    measureLatencies: Boolean
)

@JSExportAll
object MatchmakerOptions {
  val default = MatchmakerOptions(
    measureLatencies = true
  )
}

@JSExportAll
class MatchmakerMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_matchmaker_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("matchmakermultipaxos_matchmaker_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  // The number of configurations is a Gauge instead of a Counter because the
  // number of configurations can decrease when we garbage collect.
  val numConfigurations: Gauge = collectors.gauge
    .build()
    .name("matchmakermultipaxos_matchmaker_num_configurations")
    .help(
      "The number of configurations managed by this matchmaker. Garbage " +
        "collected configurations are not included in this count."
    )
    .register()

  val nacksSentTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_matchmaker_nacks_sent_total")
    .help("Total number of nacks sent.")
    .register()

  val gcWatermark: Gauge = collectors.gauge
    .build()
    .name("matchmakermultipaxos_matchmaker_gc_watermark")
    .help("The GC watermark.")
    .register()

  val staleGarbageCollectsTotal: Counter = collectors.counter
    .build()
    .name("matchmakermultipaxos_matchmaker_stale_garbage_collects_total")
    .help("Total number of stale GarbageCollects received.")
    .register()
}

@JSExportAll
class Matchmaker[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    options: MatchmakerOptions = MatchmakerOptions.default,
    metrics: MatchmakerMetrics = new MatchmakerMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {
  config.checkValid()
  logger.check(config.matchmakerAddresses.contains(address))

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = MatchmakerInbound
  override val serializer = MatchmakerInboundSerializer
  type Round = Int

  // Fields ////////////////////////////////////////////////////////////////////
  private val index = config.matchmakerAddresses.indexOf(address)

  @JSExport
  protected var gcWatermark: Int = 0

  // TODO(mwhittaker): If matchmakers are a bottleneck, we can try replacing
  // this sorted set with a regular set and with an int recording the largest
  // round in the set.
  @JSExport
  protected var configurations = mutable.SortedMap[Round, Configuration]()

  // Helpers ///////////////////////////////////////////////////////////////////
  private def timed[T](label: String)(e: => T): T = {
    if (options.measureLatencies) {
      val startNanos = System.nanoTime
      val x = e
      val stopNanos = System.nanoTime
      metrics.requestsLatency
        .labels(label)
        .observe((stopNanos - startNanos).toDouble / 1000000)
      x
    } else {
      e
    }
  }

  // Handlers //////////////////////////////////////////////////////////////////
  override def receive(src: Transport#Address, inbound: InboundMessage) = {
    import MatchmakerInbound.Request

    val label = inbound.request match {
      case Request.MatchRequest(_)   => "MatchRequest"
      case Request.GarbageCollect(_) => "GarbageCollect"
      case Request.Empty =>
        logger.fatal("Empty MatchmakerInbound encountered.")
    }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.MatchRequest(r)   => handleMatchRequest(src, r)
        case Request.GarbageCollect(r) => handleGarbageCollect(src, r)
        case Request.Empty =>
          logger.fatal("Empty MatchmakerInbound encountered.")
      }
    }
  }

  private def handleMatchRequest(
      src: Transport#Address,
      matchRequest: MatchRequest
  ): Unit = {
    val leader = chan[Leader[Transport]](src, Leader.serializer)

    // A matchmaker only processes a match request if the request's round is
    // larger than any previously seen and at least as large as the
    // gcWatermark. Otherwise, a nack is sent back.
    //
    // It's possible to implement things so that leaders re-send MatchRequests
    // if they haven't received a MatchReply for a while. If we did this, then
    // the matchmaker would have to re-send replies to requests that it has
    // already processed. We don't do this though.
    if (matchRequest.configuration.round < gcWatermark) {
      logger.debug(
        s"Matchmaker received a MatchRequest in round " +
          s"${matchRequest.configuration.round} but has a gcWatermark of " +
          s"$gcWatermark, so the request is being ignored, and a nack is " +
          s"being sent."
      )
      leader.send(
        LeaderInbound()
          .withMatchmakerNack(MatchmakerNack(round = gcWatermark - 1))
      )
      metrics.nacksSentTotal.inc()
      return
    }

    if (!configurations.isEmpty &&
        matchRequest.configuration.round <= configurations.lastKey) {
      logger.debug(
        s"Matchmaker received a MatchRequest in round " +
          s"${matchRequest.configuration.round} but has already processed a " +
          s"MatchRequest in round ${configurations.lastKey}, so the request " +
          s"is being ignored, and a nack is being sent back."
      )
      leader.send(
        LeaderInbound()
          .withMatchmakerNack(MatchmakerNack(round = configurations.lastKey))
      )
      metrics.nacksSentTotal.inc()
      return
    }

    // Send back all previous acceptor groups and store the new acceptor group.
    leader.send(
      LeaderInbound().withMatchReply(
        MatchReply(
          round = matchRequest.configuration.round,
          matchmakerIndex = index,
          gcWatermark = gcWatermark,
          configuration = configurations.values.toSeq
        )
      )
    )
    configurations(matchRequest.configuration.round) =
      matchRequest.configuration
    metrics.numConfigurations.set(configurations.size)
  }

  private def handleGarbageCollect(
      src: Transport#Address,
      garbageCollect: GarbageCollect
  ): Unit = {
    // Send back an ack. Note that we don't ignore stale requests. If we did
    // this, then it's possible a leader with a stale gcWatermark would be
    // completely ignored by the acceptors, which is not what we want.
    gcWatermark = Math.max(gcWatermark, garbageCollect.gcWatermark)
    val leader = chan[Leader[Transport]](src, Leader.serializer)
    leader.send(
      LeaderInbound().withGarbageCollectAck(
        GarbageCollectAck(
          matchmakerIndex = index,
          gcWatermark = gcWatermark
        )
      )
    )

    // Garbage collect configurations.
    configurations = configurations.dropWhile({
      case (round, _) => round < gcWatermark
    })
    metrics.gcWatermark.set(gcWatermark)
  }
}
