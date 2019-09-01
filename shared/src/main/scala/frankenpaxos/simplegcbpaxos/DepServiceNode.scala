package frankenpaxos.simplegcbpaxos

import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import frankenpaxos.monitoring.Summary
import frankenpaxos.statemachine.StateMachine
import frankenpaxos.util
import scala.collection.mutable
import scala.scalajs.js.annotation._

@JSExportAll
object DepServiceNodeInboundSerializer
    extends ProtoSerializer[DepServiceNodeInbound] {
  type A = DepServiceNodeInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
case class DepServiceNodeOptions(
    // A dependency service node garbage collects its conflict index every
    // `garbageCollectEveryNCommands` commands that it receives.
    garbageCollectEveryNCommands: Int,
    // If true, the dependency service node records how long various things
    // take to do and reports them using the
    // `simple_gc_bpaxos_dep_service_node_requests_latency` metric.
    measureLatencies: Boolean,
    // If `unsafeReturnNoDependencies` is true, dependency service nodes return
    // no dependencies for every command. As the name suggests, this is unsafe
    // and breaks the protocol. It should be used only for performance
    // debugging and evaluation.
    unsafeReturnNoDependencies: Boolean
)

@JSExportAll
object DepServiceNodeOptions {
  val default = DepServiceNodeOptions(
    garbageCollectEveryNCommands = 100,
    measureLatencies = true,
    unsafeReturnNoDependencies = false
  )
}

@JSExportAll
class DepServiceNodeMetrics(collectors: Collectors) {
  val requestsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_dep_service_node_requests_total")
    .labelNames("type")
    .help("Total number of processed requests.")
    .register()

  val requestsLatency: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_dep_service_node_requests_latency")
    .labelNames("type")
    .help("Latency (in milliseconds) of a request.")
    .register()

  val dependencies: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_dep_service_node_dependencies")
    .help(
      "The number of dependencies that a dependency service node computes " +
        "for a command. Note that the number of dependencies might be very " +
        "large, but in reality is represented compactly as a smaller set."
    )
    .register()

  val uncompactedDependencies: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_dep_service_node_uncompacted_dependencies")
    .help(
      "The number of uncompacted dependencies that a dependency service node " +
        "computes for a command. This is the number of dependencies that " +
        "cannot be represented compactly."
    )
    .register()

  val snapshotsTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_dep_service_node_snapshots_total")
    .help("Total number of snapshot requests received.")
    .register()

  val snapshotDependencies: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_dep_service_node_snapshot_dependencies")
    .help(
      "The number of dependencies that a dependency service node computes " +
        "for a snapshot. Note that the number of dependencies might be very " +
        "large, but in reality is represented compactly as a smaller set."
    )
    .register()

  val uncompactedSnapshotDependencies: Summary = collectors.summary
    .build()
    .name("simple_gc_bpaxos_dep_service_node_uncompacted_snapshot_dependencies")
    .help(
      "The number of uncompacted dependencies that a dependency service node " +
        "computes for a snapshot. This is the number of dependencies that " +
        "cannot be represented compactly."
    )
    .register()

  val garbageCollectionTotal: Counter = collectors.counter
    .build()
    .name("simple_gc_bpaxos_dep_service_node_garbage_collection_total")
    .help("Total number of garbage collections performed.")
    .register()
}

@JSExportAll
object DepServiceNode {
  val serializer = DepServiceNodeInboundSerializer
}

@JSExportAll
class DepServiceNode[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    logger: Logger,
    config: Config[Transport],
    stateMachine: StateMachine,
    options: DepServiceNodeOptions = DepServiceNodeOptions.default,
    metrics: DepServiceNodeMetrics = new DepServiceNodeMetrics(
      PrometheusCollectors
    )
) extends Actor(address, transport, logger) {
  import DepServiceNode._

  // Types /////////////////////////////////////////////////////////////////////
  override type InboundMessage = DepServiceNodeInbound
  override def serializer = DepServiceNode.serializer

  // Fields ////////////////////////////////////////////////////////////////////
  // Sanity check the configuration and get our index.
  logger.check(config.valid())
  logger.check(config.depServiceNodeAddresses.contains(address))
  private val index = config.depServiceNodeAddresses.indexOf(address)

  // This compacted conflict index stores all of the commands seen so far. When
  // a dependency service node receives a new command, it uses the conflict
  // index to efficiently compute dependencies.
  @JSExport
  protected val conflictIndex =
    new CompactConflictIndex(config.leaderAddresses.size, stateMachine)

  // The number of commands that the dependency service node has received since
  // the last time it garbage collected.  Every
  // `options.garbageCollectEveryNCommands` commands, this value is reset and
  // the conflict index is garbage collected.
  @JSExport
  protected var numCommandsPendingGc: Int = 0

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
  override def receive(
      src: Transport#Address,
      inbound: DepServiceNodeInbound
  ): Unit = {
    import DepServiceNodeInbound.Request

    val label = inbound.request match {
      case Request.DependencyRequest(_) =>
        "DependencyRequest"
      case Request.Empty => {
        logger.fatal("Empty DepServiceNodeInbound encountered.")
      }
    }
    metrics.requestsTotal.labels(label).inc()

    timed(label) {
      inbound.request match {
        case Request.DependencyRequest(r) =>
          handleDependencyRequest(src, r)
        case Request.Empty => {
          logger.fatal("Empty DepServiceNodeInbound encountered.")
        }
      }
    }
  }

  private def handleDependencyRequest(
      src: Transport#Address,
      dependencyRequest: DependencyRequest
  ): Unit = {
    // If options.unsafeReturnNoDependencies is true, we return no dependencies
    // and return immediately. This makes the protocol unsafe, but is useful
    // for performance debugging.
    val leader = chan[Leader[Transport]](src, Leader.serializer)
    if (options.unsafeReturnNoDependencies) {
      leader.send(
        LeaderInbound().withDependencyReply(
          DependencyReply(
            vertexId = dependencyRequest.vertexId,
            depServiceNodeIndex = index,
            dependencies =
              VertexIdPrefixSet(config.leaderAddresses.size).toProto()
          )
        )
      )
      return
    }

    import CommandOrSnapshot.Value
    val dependencies = dependencyRequest.commandOrSnapshot.value match {
      case Value.Snapshot(_) =>
        metrics.snapshotsTotal.inc()
        val dependencies = timed("DependencyRequest/snapshotDependencies") {
          val dependencies = conflictIndex.highWatermark()
          dependencies.subtractOne(dependencyRequest.vertexId)
          dependencies
        }
        conflictIndex.putSnapshot(dependencyRequest.vertexId)
        metrics.snapshotDependencies.observe(dependencies.size)
        metrics.uncompactedSnapshotDependencies.observe(
          dependencies.uncompactedSize
        )
        dependencies

      case Value.Command(command) =>
        val bytes = command.command.toByteArray
        var dependencies = timed("DependencyRequest/computeDependencies") {
          val dependencies = conflictIndex.getConflicts(bytes)
          dependencies.subtractOne(dependencyRequest.vertexId)
          dependencies
        }
        conflictIndex.put(dependencyRequest.vertexId, bytes)
        metrics.dependencies.observe(dependencies.size)
        metrics.uncompactedDependencies.observe(dependencies.uncompactedSize)
        dependencies

      case Value.Empty =>
        logger.fatal("DepServiceNode received empty CommandOrSnapshot.")
    }

    timed("DependencyRequest/send") {
      leader.send(
        LeaderInbound().withDependencyReply(
          DependencyReply(vertexId = dependencyRequest.vertexId,
                          depServiceNodeIndex = index,
                          dependencies = dependencies.toProto())
        )
      )
    }

    numCommandsPendingGc += 1
    if (numCommandsPendingGc % options.garbageCollectEveryNCommands == 0) {
      timed("DependencyRequest/garbageCollect") {
        conflictIndex.garbageCollect()
      }
      metrics.garbageCollectionTotal.inc()
      numCommandsPendingGc = 0
    }
  }
}