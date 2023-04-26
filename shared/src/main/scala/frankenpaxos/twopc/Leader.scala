package frankenpaxos.twopc

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
import com.github.tototoshi.csv.CSVWriter

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
  val twoPCRequestsTotal: Counter = collectors.counter
    .build()
    .name("twopc_leader_requests_total")
    .help("Total client requests.")
    .register()
}

@JSExportAll
class Leader[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    config: Config[Transport],
    transport: Transport,
    logger: Logger,
    writer: CSVWriter,
    metrics: LeaderMetrics = new LeaderMetrics(PrometheusCollectors)
) extends Actor(address, transport, logger) {

  case class Request(id: Long, client: ByteString, command: ByteString)

  override type InboundMessage = LeaderInbound
  override def serializer = Leader.serializer

  private val replicas: Seq[Chan[Replica[Transport]]] =
    for (address <- config.replicaAddresses)
      yield chan[Replica[Transport]](address, Replica.serializer)

  private val clients = mutable.Map[ByteString, Chan[Client[Transport]]]()
  private val p1Votes = mutable.Map[Request, Int]()
  private val p2Votes = mutable.Map[Request, Int]()

  val flushEveryN = 15
  private var numMessagesSinceLastFlush = 0

  override def receive(
      src: Transport#Address,
      inbound: LeaderInbound
  ): Unit = {
    inbound.request match {
      case LeaderInbound.Request.P1Reply(payload) =>
        val request = Request(payload.id, payload.clientAddress, payload.command)
        if (!p1Votes.contains(request)) {
          return
        }

        if (payload.prepared) {
          p1Votes.put(request, p1Votes(request) + 1)
          if (p1Votes(request) == replicas.size) {
            p1Votes.remove(request)

            p2Votes.put(request, 0)

            writer.writeRow(
              Seq(payload.clientAddress.toString, payload.id.toString, payload.command.toString)
            )
            writer.flush()

            numMessagesSinceLastFlush += replicas.length
            replicas.foreach(
              _.sendNoFlush(
                ReplicaInbound().withP2Request(
                  P2Request(id = payload.id,
                            clientAddress = payload.clientAddress,
                            command = payload.command
                  )
                )
              )
            )
          }
        } else {
          p1Votes.remove(request)
          numMessagesSinceLastFlush += 1
          clients(payload.clientAddress).sendNoFlush(
            ClientReply(id = payload.id, committed = false, command = payload.command)
          )
        }

      case LeaderInbound.Request.P2Reply(payload) =>
        val request = Request(payload.id, payload.clientAddress, payload.command)
        if (!p2Votes.contains(request)) {
          return
        }

        if (payload.committed) {
          p2Votes.put(request, p2Votes(request) + 1)
          if (p2Votes(request) == replicas.size) {
            p2Votes.remove(request)
            numMessagesSinceLastFlush += 1
            clients(payload.clientAddress).sendNoFlush(
              ClientReply(id = payload.id, committed = true, command = payload.command)
            )
          }
        } else {
          p2Votes.remove(request)
          numMessagesSinceLastFlush += 1
          clients(payload.clientAddress).sendNoFlush(
            ClientReply(id = payload.id, committed = false, command = payload.command)
          )
        }

      case LeaderInbound.Request.ClientRequest(payload) =>
        val request = Request(payload.id, payload.clientAddress, payload.command)
        metrics.twoPCRequestsTotal.inc()
        if (!clients.contains(payload.clientAddress)) {
          clients.put(payload.clientAddress, chan[Client[Transport]](src, Client.serializer))
        }

        p1Votes.put(request, 0)

        numMessagesSinceLastFlush += replicas.length
        replicas.foreach(
          _.sendNoFlush(
            ReplicaInbound().withP1Request(
              P1Request(id = payload.id,
                        clientAddress = payload.clientAddress,
                        command = payload.command
              )
            )
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
