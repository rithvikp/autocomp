from typing import List, Dict, Any, Tuple, NamedTuple, Set, Optional, Callable, NamedTuple
import json
import subprocess
import hydro
import hydro.async_wrapper
from pathlib import Path
import paramiko
import re
from . import proc
from . import host
from . import cluster
import time
import asyncio

"""
GENERAL NOTES ABOUT PROVISIONING. Please read.

Hydro CLI provisioning assumes either all services are Scala or all services
except for the client are Hydroflow (and the client is Scala).

Instead of modifying existing experiments, simply add a new one for hydroflow with the following changes:
1. Add a cluster_spec method to the specific suite.
2. Remove the constructor and the _connect method to the overall benchmark.
3. Update the net to take in partial endpoints (and have an update method).
```
    def __init__(self, input: Input, endpoints: Dict[str, provision.EndpointProvider]) -> None:
        self._input = input
        self._endpoints = endpoints
    
    def update(self, endpoints: Dict[str, provision.EndpointProvider]) -> None:
        self._endpoints = endpoints
```
4. Update the placement method to use these endpoints, assigning ports only if necessary.
5. Add a prom_placement method to the net.
6. Add special startup code for any hydroflow processes.
7. After this startup code, call provisioner.rebuild and update the net's endpoints.
8. Only keep scala startup code for the client.
9. Temporarily, update the benchmark to set _args in the constructor and then call the super constructor.
"""
    
class EndpointProvider:
    def __init__(self):
        self._endpoints_set = None
        self._endpoints = None

    def _set_endpoints_set(self, endpoints_set: List[List[host.PartialEndpoint]]):
        self._endpoints_set = endpoints_set

    def _set_endpoints(self, endpoints: List[host.PartialEndpoint]):
        self._endpoints = endpoints

    def get(self, receiver_index: int, sender: Optional[int] = None) -> host.PartialEndpoint:
        if sender is None:
            return self._get_single(receiver_index)

        if sender == -1:
            sender = 0
        if self._endpoints_set is not None:
            return self._endpoints_set[sender][receiver_index]
        return self._endpoints[receiver_index]

    def _get_single(self, receiver_index: int) -> host.PartialEndpoint:
        if self._endpoints_set is not None:
            if len(self._endpoints_set) != 1:
                raise Exception(f"Expected only one sender; found {len(self._endpoints_set)}. Make sure to set sender=-1 when in prom_placement.")
            return self._endpoints_set[0][receiver_index]
        return self._endpoints[receiver_index]

    def get_range(self, receiver_end_index: int, sender: Optional[int] = None) -> List[host.PartialEndpoint]:
        if sender is None:
            return self._get_range_single(receiver_end_index)

        if sender == -1:
            sender = 0
        if self._endpoints_set is not None:
            assert  receiver_end_index <= len(self._endpoints_set[sender]), "Insufficient nodes were allocated"
            return self._endpoints_set[sender][0:receiver_end_index]
        
        assert receiver_end_index <= len(self._endpoints), "Insufficient nodes were allocated"
        return self._endpoints[0:receiver_end_index]

    def _get_range_single(self, receiver_end_index: int) -> List[host.PartialEndpoint]:
        if self._endpoints_set is not None:
            assert len(self._endpoints_set) == 1
            assert receiver_end_index <= len(self._endpoints_set[0])
            return self._endpoints_set[0][0:receiver_end_index]

        assert receiver_end_index <= len(self._endpoints), "Insufficient nodes were allocated"
        return self._endpoints[0:receiver_end_index]
    

    def __str__(self):
        if self._endpoints_set is not None:
            out = "{"
            for i, endpoints in enumerate(self._endpoints_set):
                out += f"{i}: ["
                for e in endpoints:
                    out += f"{str(e)}, "
                out += "], "
            out += "}"
            return out
        elif self._endpoints is not None:
            out = "["
            for e in self._endpoints:
                out += f"{str(e)}, "
            out += "]"
            return out
        return "[]"


class State:
    def __init__(self, config: Dict[str, Any], spec: Dict[str, Dict[str, int]], args):
        self._config = config
        self._spec = spec
        self._args = args

    def identity_file(self) -> str:
        return self._args['identity_file']

    def cluster_definition(self) -> Dict[str, Dict[str, List[str]]]:
        raise NotImplementedError()

    def popen_hydroflow(self, bench, label: str, f: int, args: List[str], example: Optional[str] = None) -> proc.Proc:
        raise NotImplementedError()

    def post_benchmark(self):
        pass

    def rebuild(self, f: int, connections: Dict[str, List[str]], custom_service_counts: Dict[str, int]) -> Tuple[Dict[str, EndpointProvider], List[List[host.Endpoint]]]:
        raise NotImplementedError()

    def stop(self):
        pass


def do(config: Dict[str, Any], spec: Dict[str, Dict[str, int]], args) -> State:
    if config["mode"] == "manual":
        if config["env"]["cloud"] == "gcp":
            state: State = ManualState(config, spec, args)
        else:
            raise ValueError(f'Unsupported cloud: {config["cloud"]}')
    elif config["mode"] == "hydro":
        state = HydroState(config, spec, args)
    else:
        raise ValueError(f'Unsupported mode: {config["mode"]}')

    cluster = state.cluster_definition()
    with open(args['cluster'], 'w+') as f:
        f.truncate()
        json.dump(cluster, f)

    return state

def gcp_instance_group_ips(group_name: str, zone: str) -> List[str]:
    # TODO: The CLI is used rather than the SDK as a temporary hack
    cmd = ['gcloud', 'compute', 'instances', 'list', '--filter', f'name~"{group_name}-.*"', '--zones', zone, '--format', 'table(networkInterfaces[].networkIP.notnull().list())']

    ips = subprocess.check_output(cmd).decode('utf8').strip().split('\n')[1:]
    return ips

def cluster_definition_from_ips(spec: Dict[str, Dict[str, int]], ips: List[str]) -> Dict[str, Dict[str, List[str]]]:
        clusters: Dict[str, Dict[str, List[str]]] = {}
        for f, cluster in spec.items():
            clusters[f] = {}
            i = 0
            for role, count in cluster.items():
                if count > len(ips) - i:
                    raise ValueError(f'Not enough IPs to satisfy cluster spec')
                clusters[f][role] = ips[i:i+count]
                i += count
        return clusters
        

class ManualState(State):
    def __init__(self, config: Dict[str, Any], spec: Dict[str, Dict[str, int]], args):
        super().__init__(config, spec, args)
        self._ips = gcp_instance_group_ips(config["manual"]["instance_group_name"], config["env"]["zone"])

    def cluster_definition(self) -> Dict[str, Dict[str, List[str]]]:
        return cluster_definition_from_ips(self._spec, self._ips)

class HydroState(State):
    def __init__(self, config: Dict[str, Any], spec: Dict[str, Dict[str, int]], args):
        super().__init__(config, spec, args)
        if config["env"]["cloud"] != "gcp" and config["env"]["cloud"] != "local":
            raise ValueError(f'Unsupported cloud for Hydro CLI deployments: {config["env"]["cloud"]}')

        # Mapping from f to role to machines
        self._machines: Dict[str, Dict[str, List[hydro.Host]]] = {}
        self._identity_file = ""

        hydro.async_wrapper.run(self._configure, None)
        # Mapping from role to a set of initialized indices
        self._service_exists: Dict[str, Set[int]] = {}
        self._services: Dict[str, List[hydro.Service]] = {}
        self.reset_services()

        self._conn_cache = cluster._RemoteHostCache(self._ssh_connect)
        self._connect_to_all()
    
    def identity_file(self) -> str:
        if self._identity_file == "":
            return super().identity_file()
        return self._identity_file

    def _internal_ip(self, m: hydro.Host) -> str:
        if hasattr(m, 'internal_ip'):
            return m.internal_ip
        return 'localhost'
    
    def _connect_to_all(self):
        for _, machines in self._machines.items():
            for _, machines in machines.items():
                for m in machines:
                    self._conn_cache.connect(self._internal_ip(m))

    def hosts(self, f: int) -> Dict[str, EndpointProvider]:
        hosts = {}
        for role, machines in self._machines[str(f)].items():
            endpoints = [host.PartialEndpoint(self._conn_cache.connect(self._internal_ip(m)), None) for m in machines]
            provider = EndpointProvider()
            provider._set_endpoints(endpoints)
            hosts[role] = provider
        return hosts

    def _ssh_connect(self, address: str) -> host.Host:
        tries = 0
        # Unfortunately, there seems to somtimes be a delay between a VM starting up and it being
        # accessible via SSH, so wait and then retry if a connection attempt fails.
        while tries < 5:
            try:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)

                if self._identity_file:
                    client.connect(address, key_filename=self._identity_file, username=self._config["env"]["user"])
                else:
                    client.connect(address)
                return host.RemoteHost(client)
            except:
                tries += 1
                time.sleep(10)

        raise Exception(f'Failed to connect to instance {address} after 5 attempts')


    def reset_services(self):
        self._custom_ports = HydroState.CustomPorts(send={}, receive={})
        self._services = {}
        for _, cluster in self._spec.items():
            for role, _ in cluster.items():
                self._service_exists[role] = set()
                self._services[role] = []

    async def _configure(self, _):
        """Perform initial configuration for an experiment, only deploying machines.
        """
        self._deployment = hydro.Deployment()
        deployment = self._deployment
        config = self._config

        if config["env"]["cloud"] == "gcp":
            gcp_vpc = hydro.GCPNetwork(
                project=self._config["env"]["project"],
                existing=self._config["env"]["vpc"],
            )
            self._gcp_vpc = gcp_vpc

        last_machine = None
        custom_services = []
        if config["env"]["cloud"] == "local":
            # FIXME[Hydro CLI]: Remove once multiple localhost objects can be used
            localhost = deployment.Localhost()
        for f, cluster in self._spec.items():
            self._machines[f] = {}
            for role, count in cluster.items():
                hydroflow = config["services"][role]["type"] == "hydroflow"
                if config["env"]["cloud"] == "gcp":
                    if hydroflow:
                        image = config["env"]["hydroflow_image"]
                    else:
                        image = config["env"]["scala_image"]
                self._machines[f][role] = []

                # count is either a string (the name of the role it's co-located with) or an int (the number of machines)
                if isinstance(count, str):
                    continue

                for i in range(count):
                    if config["env"]["cloud"] == "gcp":
                        machine = deployment.GCPComputeEngineHost(
                            project=config["env"]["project"],
                            machine_type=config["env"]["machine_type"],
                            image=image,
                            region=config["env"]["zone"],
                            network=gcp_vpc,
                            user=config["env"]["user"],
                        )
                    elif config["env"]["cloud"] == "local":
                        machine = localhost

                    # FIXME[Hydro CLI]: Change this once ports can be opened after a machine
                    # is started or port opening can be disabled when creating HydroflowCrate services.
                    custom_services.append(deployment.CustomService(machine, [22]))

                    self._machines[f][role].append(machine)
                    last_machine = machine

            # 2nd pass, since count is either a string (the name of the role it's co-located with) or an int (the number of machines)
            for role, colocated_role in cluster.items():
                if isinstance(colocated_role, str):
                    for machine in self._machines[f][colocated_role]:
                        self._machines[f][role].append(machine)
                        
        await deployment.deploy()
        await deployment.start()

        if last_machine is None:
            raise ValueError("No machines were provisioned")
        if hasattr(last_machine, 'ssh_key_path'):
            self._identity_file = last_machine.ssh_key_path
    

    def cluster_definition(self) -> Dict[str, Dict[str, List[str]]]:
        clusters: Dict[str, Dict[str, List[str]]] = {}
        for f, cluster in self._machines.items():
            clusters[f] = {}
            for role, hosts in cluster.items():
                clusters[f][role] = []
                for host in hosts:
                    clusters[f][role].append(self._internal_ip(host))

        return clusters
    
    def _parse_label(self, label: str) -> Tuple[str, int]:
        decomposed_label = re.match(r"([a-zA-Z0-9_]+)_([0-9]+)", label)
        if decomposed_label is None:
            decomposed_label = re.match(r"([a-zA-Z0-9]+)", label)
            assert decomposed_label is not None, "The provided label was invalid. Check the comment in provision.py"
            index = 0
        else:
            index = int(decomposed_label[2])
        role = decomposed_label[1]

        if index in self._service_exists[role]:
            raise ValueError(f"The same label {label} was provisioned twice. Are your labels of the form `role_index`?")
        self._service_exists[role].add(index)

        return role, index

    # It is assumed that label is of the form f"{role}_{index}"
    def popen_hydroflow(self, bench, label: str, f: int, args: List[str], example: Optional[str] = None) -> proc.Proc:
        """Start a new hydroflow program on a free machine.
        """
        role, index = self._parse_label(label)
        assert self._config["services"][role]["type"] == "hydroflow"

        if example is None:
            example = self._config["services"][role]["rust_example"]
        machine = self._machines[str(f)][role][index]

        # FIXME[Hydro CLI]: Pipe stdout/stderr to files
        stdout = bench.abspath(f'{label}_out.txt')
        stderr = bench.abspath(f'{label}_err.txt')
        receiver = self._deployment.HydroflowCrate(
            src=str((Path(__file__).parent.parent / "rust").absolute()),
            example=example,
            args = args,
            # display_id = label,
            on=machine,
        )
        self._services[role].append(receiver)

        return proc.HydroflowProc(receiver)

    def post_benchmark(self):
        """Stop all services and reset the state of the provisioner.

        This method should be called after every benchmark.
        """
        async def stop(_):
            for role, s in self._services.items():
                for service in s:
                    await service.stop()
        
        asyncio.set_event_loop(asyncio.new_event_loop())
        hydro.async_wrapper.run(stop, None)

        self.reset_services()

    class CustomPorts(NamedTuple):
        send: 'Dict[str, Tuple[str, List[Any]]]'
        receive: 'Dict[str[Tuple[str, List[List[hydro.CustomServicePort]]]]]'

    def rebuild(self, f: int, connections: Dict[str, List[str]], custom_service_counts: Dict[str, int]) -> Tuple[Dict[str, EndpointProvider], List[List[host.Endpoint]]]:
        """Given the specified connections, pass them to hydro CLI and deploy all services.

        Returns a mapping between every role and a list of endpoints (for Scala clients to communicate
        with) as well as a separate list of endpoints (back channels) with which Scala clients should
        open connections (but not send data).

        Connections should be in the form {sender: [receiver1, receiver2, ...], ...}. Note that the same receiver
        can be specified multiple times, meaning multiple channels will be opened.
        """
        # Create faux services for any scala services
        for role, spec in self._config["services"].items():
            if spec["type"] != "scala":
                continue
            for machine in self._machines[str(f)][role][:custom_service_counts[role]]:
                self._services[role].append(self._deployment.CustomService(machine, []))

        # Sending and receiving is with respect to the custom service
        # self._custom_ports: Dict[str, Dict[str, Tuple[str, List[List[hydro.CustomServicePort]]]]] = {"send": {}, "receive": {}}
        self._custom_ports = HydroState.CustomPorts(send={}, receive={})

        # For every connection pair, generate the correct set of ports and link them together
        for sender, conn_receivers in connections.items():
            assert sender in self._services, f"Sender {sender} is not a valid role"
            seen_receivers: Dict[str, int] = {}
            for receiver in conn_receivers:
                assert receiver in self._services, f"Receiver {receiver} is not a valid role"

                index = seen_receivers.get(receiver, 0)
                seen_receivers[receiver] = index + 1

                sender_hf = self._config["services"][sender]["type"] == "hydroflow"
                receiver_hf = self._config["services"][receiver]["type"] == "hydroflow"
                if not sender_hf and not receiver_hf:
                    continue

                # Generate a list of receiver ports, one for each service in the given role.
                if receiver_hf:
                    receivers = [getattr(n.ports, f'receive_from${sender}${index}').merge() for n in self._services[receiver]]
                else:
                    assert sender not in self._custom_ports.receive, f"Hyrdoflow process {sender} can only send to one custom service"
                    self._custom_ports.receive[sender] = (receiver, [])
                    receivers = []
                    for s in self._services[receiver]:
                        ports = []
                        for _ in range(len(self._services[sender])):
                            receiver_port = s.client_port()
                            receivers.append(receiver_port)
                            # Custom ports need to be tracked separately so the final port allocations can be discovered.
                            ports.append(receiver_port)
                        self._custom_ports.receive[sender][1].append(ports)

                # Generate a list of sender ports, one for each service in the given role.
                senders = []
                if sender_hf:
                    for s in self._services[sender]:
                        sender_port = getattr(s.ports, f'send_to${receiver}${index}')
                        senders.append(sender_port)
                else:
                    assert receiver not in self._custom_ports.send, f"Hydroflow process {receiver} can only receive from one custom service"
                    self._custom_ports.send[receiver] = (sender, [])

                    for s in self._services[sender]:
                        sender_port = s.client_port()
                        senders.append(sender_port)
                        self._custom_ports.send[receiver][1].append(sender_port)

                # Connect the senders to the receivers
                if receiver_hf:
                    demux = hydro.demux({
                        i: r for i, r in enumerate(receivers)
                    })
                    for i, s in enumerate(senders):
                        s.tagged(i).send_to(demux)
                else:
                    # This special case is needed since custom ports can't be used with tagged senders
                    for i, s in enumerate(senders):
                        receiver_ports = []
                        for j in range(i, len(receivers), len(senders)):
                            receiver_ports.append(receivers[j])
                        demux = hydro.demux({
                            j: r for j, r in enumerate(receiver_ports)
                        })
                        s.send_to(demux)
        
        asyncio.set_event_loop(asyncio.new_event_loop())
        hydro.async_wrapper.run(self._redeploy_and_start, None)

        # All services have been deployed so collect the endpoints that need to be returned.
        # The instance variables were populated by self._redeploy_and_start()
        endpoints: Dict[str, EndpointProvider] = {}
        for role, spec in self._config["services"].items():
            # endpoints[role] = 
            role_endpoints = []
            provider = EndpointProvider()
            if spec["type"] == "scala":
                for machine in self._machines[str(f)][role][:custom_service_counts[role]]:
                    role_endpoints.append(host.PartialEndpoint(
                        host=self._conn_cache.connect(self._internal_ip(machine)),
                        port=None,
                    ))
                provider._set_endpoints(role_endpoints)
            elif spec["type"] == "hydroflow":
                if role not in self._hydroflow_endpoints:
                    role_endpoints = []
                    for i in range(len(self._services[role])):
                        machine = self._machines[str(f)][role][i]
                        role_endpoints.append(host.PartialEndpoint(
                            host=host.FakeHost(self._internal_ip(machine)),
                            port=None,
                        ))
                    provider._set_endpoints(role_endpoints)
                else:
                    provider._set_endpoints_set(self._hydroflow_endpoints[role])
            else:
                raise ValueError(f"Unrecognized service type for role {role}: {spec['type']}")
            endpoints[role] = provider

        assert len(self._hydroflow_receive_endpoints) == 1, "Only one custom service can currently receive from any hydroflow service"
        receive_endpoints = list(self._hydroflow_receive_endpoints.values())[0]

        # print("extra endpoints:", receive_endpoints)
        # print("Endpoints:")
        # for role, ep in endpoints.items():
        #     print(role, ep)

        return endpoints, receive_endpoints

    async def _redeploy_and_start(self, _):
        """Redeploy and start the services and collect any connection endpoints for custom ports.
        """
        await self._deployment.deploy()
        await self._deployment.start()

        self._hydroflow_endpoints: Dict[str, List[List[host.PartialEndpoint]]] = {}
        self._hydroflow_receive_endpoints: Dict[str, List[List[host.Endpoint]]] = {}

        # Gather all the endpoints for scala --> hydroflow connections
        for receiver, sender_ports in self._custom_ports.send.items():
            # Sender is always a custom service and receiver is always a hydroflow service
            assert receiver not in self._hydroflow_endpoints, (f"Multiple scala roles cannot "+
                f"currently send to the same hydroflow process: {receiver}")
            self._hydroflow_endpoints[receiver] = []
            sender = sender_ports[0]
            for port in sender_ports[1]:
                addrs = (await port.server_port()).json()['Demux']
                assert len(addrs) == len(self._services[receiver]), (f"The number of "+
                    f"addresses does not match the number of nodes with the role {receiver}.")

                endpoints  = [None]*len(addrs)
                for index, addr in addrs.items():
                    addr = addr["TcpPort"]
                    decomposed_addr = re.match(r"([0-9.]+):([0-9]+)", addr)
                    assert decomposed_addr is not None, f"Could not parse address {addr}"
                    
                    endpoints[index] = host.PartialEndpoint(
                        host.FakeHost(decomposed_addr[1]),
                        int(decomposed_addr[2]),
                    )
                self._hydroflow_endpoints[receiver].append(endpoints)

        # Gather all the endpoints for hydroflow --> scala connections
        # These are back channels on which Scala will not send messages (but hydroflow will)
        assert len(self._custom_ports.receive) == 1, "Only one scala service can currently receive from any hydroflow service"
        for sender, receiver_ports in self._custom_ports.receive.items():
            receiver = receiver_ports[0]
            # Sender is always a hydroflow service and receiver is always a custom service
            self._hydroflow_receive_endpoints[sender] = []
            for ports in receiver_ports[1]:
                endpoints = []
                for port in ports:
                    addr = (await port.server_port()).json()
                    addr = addr["TcpPort"]
                    decomposed_addr = re.match(r"([0-9.]+):([0-9]+)", addr)
                    assert decomposed_addr is not None, f"Could not parse address {addr}"

                    endpoints.append(host.Endpoint(
                        host.FakeHost(decomposed_addr[1]),
                        int(decomposed_addr[2]),
                    ))
                self._hydroflow_receive_endpoints[sender].append(endpoints)

    
    def stop(self):
        self._custom_ports = HydroState.CustomPorts(send={}, receive={})
        self._services = None
        self._machines = None
        self._gcp_vpc = None
        self._deployment = None