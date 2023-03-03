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

  case class RequestId(id: Long, client: ByteString)

  override type InboundMessage = LeaderInbound
  override def serializer = Leader.serializer

  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (address <- config.replicaAddresses)
      yield chan[Replica[Transport]](address, Replica.serializer)

  private val clients = mutable.Map[ByteString, Chan[Client[Transport]]]()
  private val votes = mutable.Map[RequestId, Int]()

  override def receive(
      src: Transport#Address,
      inbound: LeaderInbound
  ): Unit = {
    inbound.request match {
      case LeaderInbound.Request.VoteReply(reply) =>
        val requestId = RequestId(reply.id, reply.clientAddress)
        if (!votes.contains(requestId)) {
          return
        }

        if (reply.accepted) {
          votes.put(RequestId(reply.id, reply.clientAddress),
                    votes(RequestId(reply.id, reply.clientAddress)) + 1
          )
          if (votes(requestId) == replicas.size) {
            clients(reply.clientAddress).send(
              ClientReply(id = reply.id, accepted = true)
            )
          }
        } else {
          votes.remove(requestId)
          clients(reply.clientAddress).send(
            ClientReply(id = reply.id, accepted = false)
          )
        }

      case LeaderInbound.Request.ClientRequest(request) =>
        metrics.votingRequestsTotal.inc()
        if (!clients.contains(request.clientAddress)) {
          clients.put(request.clientAddress, chan[Client[Transport]](src, Client.serializer))
        }

        votes.put(RequestId(request.id, request.clientAddress), 0)

        replicas.foreach(
          _.send(
            VoteRequest(id = request.id, clientAddress = request.clientAddress)
          )
        )

      case LeaderInbound.Request.Empty =>
        logger.fatal("Empty LeaderInbound encountered.")
    }
  }
}
