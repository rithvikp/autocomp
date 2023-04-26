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
import com.github.tototoshi.csv.CSVWriter

@JSExportAll
object ReplicaInboundSerializer extends ProtoSerializer[ReplicaInbound] {
  type A = ReplicaInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Replica {
  val serializer = ReplicaInboundSerializer
}

@JSExportAll
class Replica[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    config: Config[Transport],
    transport: Transport,
    logger: Logger,
    writer: CSVWriter
) extends Actor(address, transport, logger) {

  override type InboundMessage = ReplicaInbound
  override def serializer = Replica.serializer

  private val leader =
    chan[Leader[Transport]](config.leaderAddress, Leader.serializer)

  override def receive(
      src: Transport#Address,
      inbound: ReplicaInbound
  ): Unit = {
    inbound.request match {
      case ReplicaInbound.Request.P1Request(payload) =>
        writer.writeRow(
          Seq(payload.clientAddress.toString, payload.id.toString, payload.command.toString)
        )
        writer.flush()

        leader.send(
          LeaderInbound().withP1Reply(
            P1Reply(id = payload.id,
                    clientAddress = payload.clientAddress,
                    prepared = true,
                    command = payload.command
            )
          )
        )

      case ReplicaInbound.Request.P2Request(payload) =>
        leader.send(
          LeaderInbound().withP2Reply(
            P2Reply(id = payload.id,
                    clientAddress = payload.clientAddress,
                    committed = true,
                    command = payload.command
            )
          )
        )

      case ReplicaInbound.Request.Empty =>
        logger.fatal("Empty ReplicaInbound encountered.")
    }
  }
}
