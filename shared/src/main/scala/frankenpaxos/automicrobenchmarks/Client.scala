package frankenpaxos.automicrobenchmarks

import collection.mutable
import com.github.tototoshi.csv.CSVWriter
import com.google.protobuf.ByteString
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import java.io.File
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._
import scala.util.Random
import javax.crypto.Cipher
import java.nio.ByteBuffer

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
    address: Transport#Address,
    dstAddress: Transport#Address,
    transport: Transport,
    logger: Logger,
    cipher: Cipher
) extends Actor(address, transport, logger) {
  override type InboundMessage = ClientInbound
  override def serializer = Client.serializer

  private val server =
    chan[Server[Transport]](dstAddress, Server.serializer)

  private var id: Long = 0
  private var ballot: Int = 0
  private val promises = mutable.Map[Long, Promise[Unit]]()

  override def receive(src: Transport#Address, inbound: InboundMessage): Unit = {
    System.out.println("Received message")
    inbound.request match {
      case ClientInbound.Request.ClientReply(reply) =>
        promises.get(reply.id) match {
          case Some(promise) =>
            promise.success(())
            promises -= reply.id
          case None =>
            logger.fatal(s"Received reply for unpending request ${reply.id}.")
        }

      case ClientInbound.Request.ClientNotification(notif) => {}

      case ClientInbound.Request.Empty => {
        logger.fatal("Empty ClientInbound encountered.")
      }
    }
  }

  def requestImpl(promise: Promise[Unit]): Unit = {
    // 0.5% chance of incrementing the ballot.
    if (Random.nextDouble() < 0.005) {
      ballot += 1
    }

    var payload = List.fill(1)(Random.nextPrintableChar).mkString

    var paddedPayload = payload.getBytes
    for (i <- paddedPayload.length until 8) {
      paddedPayload = paddedPayload :+ 0.toByte
    }
    val vid = ByteBuffer.wrap(paddedPayload).getLong()

    payload = payload + " " * 10

    val msg = ServerInbound(
      id = id,
      ballot = ballot,
      payload = ByteString.copyFrom(cipher.doFinal(payload.getBytes())),
      vid = vid
    )
    promises(id) = promise

    server.send(msg)

    id += 1
  }

  def request(): Future[Unit] = {
    val promise = Promise[Unit]()
    transport.executionContext.execute(() => requestImpl(promise))
    promise.future
  }
}
