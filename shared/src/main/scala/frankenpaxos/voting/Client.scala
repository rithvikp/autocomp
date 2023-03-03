package frankenpaxos.voting

import collection.mutable
import com.google.protobuf.ByteString
import com.github.tototoshi.csv.CSVWriter
import frankenpaxos.Actor
import frankenpaxos.Chan
import frankenpaxos.Logger
import frankenpaxos.ProtoSerializer
import java.io.File
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.scalajs.js.annotation._

@JSExportAll
object ClientReplySerializer extends ProtoSerializer[ClientReply] {
  type A = ClientReply
  override def toBytes(x: A): Array[Byte] = super.toBytes(x)
  override def fromBytes(bytes: Array[Byte]): A = super.fromBytes(bytes)
  override def toPrettyString(x: A): String = super.toPrettyString(x)
}

@JSExportAll
object Client {
  val serializer = ClientReplySerializer
}

@JSExportAll
class Client[Transport <: frankenpaxos.Transport[Transport]](
    address: Transport#Address,
    config: Config[Transport],
    transport: Transport,
    logger: Logger
) extends Actor(address, transport, logger) {
  override type InboundMessage = ClientReply
  override def serializer = Client.serializer

  private val leader =
    chan[Leader[Transport]](config.leaderAddress, Leader.serializer)

  private var id: Long = 0
  private val promises = mutable.Map[Long, Promise[Unit]]()
  private val addressAsBytes: ByteString =
    ByteString.copyFrom(transport.addressSerializer.toBytes(address))

  override def receive(src: Transport#Address, reply: InboundMessage): Unit = {
    promises.get(reply.id) match {
      case Some(promise) =>
        promise.success(())
        promises -= reply.id
      case None =>
        logger.fatal(s"Received reply for unpending echo ${reply.id}.")
    }
  }

  def _request(promise: Promise[Unit]): Unit = {
    leader.send(
      LeaderInbound().withClientRequest(ClientRequest(id = id, clientAddress = addressAsBytes))
    )
    promises(id) = promise
    id += 1
  }

  def request(): Future[Unit] = {
    val promise = Promise[Unit]()
    transport.executionContext.execute(() => _request(promise))
    promise.future
  }
}
