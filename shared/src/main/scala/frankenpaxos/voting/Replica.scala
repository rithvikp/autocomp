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

@JSExportAll
object VoteRequestSerializer extends ProtoSerializer[VoteRequest] {
  type A = VoteRequest
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Replica {
  val serializer = VoteRequestSerializer
}

@JSExportAll
class Replica[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    config: Config[Transport],
    transport: Transport,
    logger: Logger
) extends Actor(address, transport, logger) {

  override type InboundMessage = VoteRequest
  override def serializer = Replica.serializer

  private val leader =
    chan[Leader[Transport]](config.leaderAddress, Leader.serializer)

  override def receive(
      src: Transport#Address,
      inbound: VoteRequest
  ): Unit = {
    leader.send(
      LeaderInbound().withVoteReply(
        VoteReply(id = inbound.id, clientAddress = inbound.clientAddress, accepted = true, command = inbound.command)
      )
    )
  }
}
