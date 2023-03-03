package frankenpaxos.voting

case class Config[Transport <: frankenpaxos.Transport[Transport]](
    leaderAddress: Transport#Address,
    replicaAddresses: Seq[Transport#Address]
)
