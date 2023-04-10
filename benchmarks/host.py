from . import proc
from typing import Any, NamedTuple, Sequence, Union, Optional
import abc
import paramiko


# A Host represents a machine (potentially virtual) on which you can run
# processes. A Host may represent the local machine (LocalHost) or a remote
# machine (RemoteHost).
class Host(abc.ABC):
    @abc.abstractmethod
    def ip(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def popen(self, args: Union[str, Sequence[str]], stdout: str,
              stderr: str) -> proc.Proc:
        raise NotImplementedError()


# An endpoint is a host and port. Typically, you launch a server that listens
# at a particular endpoint.
class Endpoint(NamedTuple):
    host: Host
    port: int

    def __str__(self) -> str:
        return f"{self.host.ip()}:{self.port}"

class PartialEndpoint(NamedTuple):
    host: Host
    port: Optional[int]

    def __str__(self) -> str:
        if self.port is None:
            return f"{self.host.ip()}:<>"
        else:
            return f"{self.host.ip()}:{self.port}"

class LocalHost(Host):
    def ip(self) -> str:
        return "127.0.0.1"

    def popen(self, args: Union[str, Sequence[str]], stdout: str,
              stderr: str) -> proc.Proc:
        return proc.PopenProc(args, stdout=stdout, stderr=stderr)


class RemoteHost(Host):
    def __init__(self, client: paramiko.SSHClient) -> None:
        self.client = client

    def ip(self) -> str:
        (ip, _port) = self.client.get_transport().getpeername()
        return ip

    def popen(self, args: Union[str, Sequence[str]], stdout: str,
              stderr: str) -> proc.Proc:
        return proc.ParamikoProc(self.client,
                                 args,
                                 stdout=stdout,
                                 stderr=stderr)


class FakeHost(Host):
    def __init__(self, address: str) -> None:
        self.address = address

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, FakeHost):
            return self.address == other.address
        else:
            return False

    def ip(self) -> str:
        return self.address

    def popen(self, args: Union[str, Sequence[str]], stdout: str,
              stderr: str) -> proc.Proc:
        raise NotImplementedError()
