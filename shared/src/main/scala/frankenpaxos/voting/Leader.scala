package frankenpaxos.voting

import collection.mutable
import frankenpaxos.Actor
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import frankenpaxos.monitoring.Collectors
import frankenpaxos.monitoring.Counter
import frankenpaxos.monitoring.PrometheusCollectors
import java.net.InetAddress
import java.net.InetSocketAddress
import scala.scalajs.js.annotation._
import com.google.protobuf.ByteString

@JSExportAll
object LeaderInboundSerializer extends ProtoSerializer[LeaderInbound] {
  type A = LeaderInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Leader {
  val serializer = LeaderInboundSerializer
}

@JSExportAll
class LeaderMetrics(collectors: Collectors) {
  val votingRequestsTotal: Counter = collectors.counter
    .build()
    .name("voting_requests_total")
    .help("Total client requests.")
    .register()
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    config: Config[Transport],
    transport: Transport,
    logger: Logger,
    metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {

  case class Request(id: Long, client: ByteString, command: ByteString)

  override type InboundMessage = LeaderInbound
  override def serializer = Leader.serializer

  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (address <- config.replicaAddresses)
      yield chan[Replica[Transport]](address, Replica.serializer)

  private val clients = mutable.Map[ByteString, Chan[Client[Transport]]]()
  private val votes = mutable.Map[Request, Int]()

  val flushEveryN = 15
  private var numMessagesSinceLastFlush = 0

  override def receive(
      src: Transport#Address,
      inbound: LeaderInbound
  ): Unit = {
    inbound.request match {
      case LeaderInbound.Request.VoteReply(payload) =>
        val request = Request(payload.id, payload.clientAddress, payload.command)
        if (!votes.contains(request)) {
          return
        }

        if (payload.accepted) {
          votes.put(request, votes(request) + 1)
          if (votes(request) == replicas.size) {
            votes.remove(request)
            numMessagesSinceLastFlush += 1
            clients(payload.clientAddress).sendNoFlush(
              ClientReply(id = payload.id, accepted = true, command = payload.command)
            )
          }
        } else {
          votes.remove(request)
          numMessagesSinceLastFlush += 1
          clients(payload.clientAddress).sendNoFlush(
            ClientReply(id = payload.id, accepted = false, command = payload.command)
          )
        }

      case LeaderInbound.Request.ClientRequest(payload) =>
        val request = Request(payload.id, payload.clientAddress, payload.command)
        metrics.votingRequestsTotal.inc()
        if (!clients.contains(payload.clientAddress)) {
          clients.put(payload.clientAddress, chan[Client[Transport]](src, Client.serializer))
        }

        votes.put(request, 0)

        numMessagesSinceLastFlush += replicas.length
        replicas.foreach(
          _.sendNoFlush(
            VoteRequest(id = payload.id, clientAddress = payload.clientAddress, command = payload.command)
          )
        )

      case LeaderInbound.Request.Empty =>
        logger.fatal("Empty LeaderInbound encountered.")
    }

    if (numMessagesSinceLastFlush >= flushEveryN) {
      replicas.foreach(_.flush())
      clients.foreach(_._2.flush())
      numMessagesSinceLastFlush = 0
    }
  }
}
