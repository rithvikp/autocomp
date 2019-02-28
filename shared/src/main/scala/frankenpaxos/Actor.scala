package frankenpaxos

import scala.scalajs.js.annotation._

@JSExportAll
abstract class Actor[Transport <: frankenpaxos.Transport[Transport]](
    val address: Transport#Address,
    val transport: Transport,
    val logger: Logger
) {
  // Interface.
  type InboundMessage
  def serializer: Serializer[InboundMessage]
  def receive(src: Transport#Address, message: InboundMessage): Unit

  // Implementation.
  transport.register(address, this);

  def receiveImpl(src: Transport#Address, bytes: Array[Byte]): Unit = {
    receive(src, serializer.fromBytes(bytes))
  }

  type Chan[A <: frankenpaxos.Actor[Transport]] =
    frankenpaxos.Chan[Transport, A]

  def chan[A <: frankenpaxos.Actor[Transport]](
      dst: Transport#Address,
      serializer: Serializer[A#InboundMessage]
  ): Chan[A] = {
    new Chan[A](transport, address, dst, serializer)
  }

  def send(dst: Transport#Address, bytes: Array[Byte]): Unit = {
    transport.send(address, dst, bytes)
  }

  def timer(
      name: String,
      delay: java.time.Duration,
      f: () => Unit
  ): Transport#Timer = {
    transport.timer(address, name, delay, f)
  }
}