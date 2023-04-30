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
import javax.crypto.Cipher
import javax.crypto.SecretKey
import java.security.KeyFactory
import javax.crypto.spec.SecretKeySpec
import javax.crypto.spec.IvParameterSpec
import javax.xml.bind.DatatypeConverter
import javax.crypto.spec.GCMParameterSpec

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
    logger: Logger
) extends Actor(address, transport, logger) {
  override type InboundMessage = ClientInbound
  override def serializer = Client.serializer

  private val server =
    chan[Server[Transport]](dstAddress, Server.serializer)

  private var id: Long = 0
  private var ballot: Int = 0
  private val promises = mutable.Map[Long, Promise[Unit]]()

  private val symmetricKeyBytes = DatatypeConverter.parseHexBinary(
    "bfeed277024d4700c7edf24127858917"
  )

  val key = new SecretKeySpec(symmetricKeyBytes, "AES")

  private val iv = new IvParameterSpec("unique nonce".getBytes())
  private val gcmParams = new GCMParameterSpec(128, iv.getIV())

  private val payloads = for (i <- 33 until 127) yield {
    val payload = List.fill(1)(i.toChar).mkString
    var encryptedPayload = (payload + " " * 10).getBytes
    for (i <- 0 until 100) yield {
      val cipher = Cipher.getInstance("AES/GCM/NoPadding")
      cipher.init(Cipher.ENCRYPT_MODE, key, gcmParams)
      encryptedPayload = cipher.doFinal(encryptedPayload)
    }
    ByteString.copyFrom(encryptedPayload)
  }

  override def receive(src: Transport#Address, inbound: InboundMessage): Unit = {
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

    val payloadIndex = Random.nextInt(payloads.size)

    val msg = ServerInbound(
      id = id,
      ballot = ballot,
      payload = payloads(payloadIndex),
      vid = payloadIndex
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
