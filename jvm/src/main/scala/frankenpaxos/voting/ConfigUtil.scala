package frankenpaxos.voting

import frankenpaxos.NettyTcpAddress
import frankenpaxos.NettyTcpTransport
import java.net.InetSocketAddress

object ConfigUtil {
  def fromProto(proto: NettyConfigProto): Config[NettyTcpTransport] = {
    def addr(hostPort: HostPortProto): NettyTcpAddress =
      NettyTcpAddress(new InetSocketAddress(hostPort.host, hostPort.port))

    Config[NettyTcpTransport](
      leaderAddress = addr(proto.leaderAddress),
      replicaAddresses = proto.replicaAddress.map(addr)
    )
  }

  def fromFile(filename: String): Config[NettyTcpTransport] = {
    val source = scala.io.Source.fromFile(filename)
    try {
      fromProto(NettyConfigProto.fromAscii(source.mkString))
    } finally {
      source.close()
    }
  }
}
