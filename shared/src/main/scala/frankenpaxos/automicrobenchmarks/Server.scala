package frankenpaxos.automicrobenchmarks

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
import scala.collection.mutable

@JSExportAll
object ServerInboundSerializer extends ProtoSerializer[ServerInbound] {
  type A = ServerInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Server {
  val serializer = ServerInboundSerializer
}

@JSExportAll
class Server[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    transport: Transport,
    persistLog: Boolean,
    flushEveryN: Int,
    logger: Logger
) extends Actor(address, transport, logger) {
  override type InboundMessage = ServerInbound
  override def serializer = Server.serializer

  override def receive(
      src: Transport#Address,
      request: ServerInbound
  ): Unit = {}
}
