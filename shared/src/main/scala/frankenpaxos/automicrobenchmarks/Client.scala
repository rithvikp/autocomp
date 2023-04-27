package frankenpaxos.automicrobenchmarks

import collection.mutable
import com.github.tototoshi.csv.CSVWriter
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import java.io.File
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._
import scala.util.Random

@JSExportAll
object ClientInboundSerializer extends ProtoSerializer[ClientInbound] {
  type A = ClientInbound
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Client {
  val serializer = ClientInboundSerializer
}

@JSExportAll
class Client[Transport <: frankenpaxos.Transport[Transport]](
    srcAddress: Transport#Address,
    dstAddress: Transport#Address,
    transport: Transport,
    logger: Logger
) extends Actor(srcAddress, transport, logger) {
  override type InboundMessage = ClientInbound
  override def serializer = Client.serializer

  private val server =
    chan[Server[Transport]](dstAddress, Server.serializer)

  private var id: Long = 0
  private var ballot: Int = 0
  private val promises = mutable.Map[Long, Promise[Unit]]()

  override def receive(src: Transport#Address, reply: InboundMessage): Unit = {
    promises.get(reply.id) match {
      case Some(promise) =>
        promise.success(())
        promises -= reply.id
      case None =>
        logger.fatal(s"Received reply for unpending request ${reply.id}.")
    }
  }

  def sendImpl(promise: Promise[Unit]): Unit = {
    // 0.5% chance of incrementing the ballot.
    if (Random.nextDouble() < 0.005) {
      ballot += 1
    }

    var payload = Random.nextString(6)
    payload = payload + " " * 10
    val msg = ServerInbound(
      id = id,
      ballot = ballot,
      payload = payload,
      signature = ""
    )
    promises(id) = promise

    server.send(msg)

    id += 1
  }

  def send(): Future[Unit] = {
    val promise = Promise[Unit]()
    transport.executionContext.execute(() => sendImpl(promise))
    promise.future
  }
}
