from typing import List, Dict, Any, Tuple, NamedTuple, Set, Optional, Callable
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
    def __init__(self, input: Input, endpoints: Dict[str, List[host.PartialEndpoint]]) -> None:
        self._input = input
        self._endpoints = endpoints
    
    def update(self, endpoints: Dict[str, List[host.PartialEndpoint]]) -> None:
        self._endpoints = endpoints
```
4. Update the placement method to use these endpoints, assigning ports only if necessary.
5. Add a prom_placement method to the net.
6. Add special startup code for any hydroflow processes.
7. After this startup code, call provisioner.rebuild and update the net's endpoints.
8. Only keep scala startup code for the client.
9. Temporarily, update the benchmark to set _args in the constructor and then call the super constructor.
"""

class State:
    def __init__(self, config: Dict[str, Any], spec: Dict[str, Dict[str, int]], args):
        self._config = config
        self._spec = spec
        self._args = args

    def identity_file(self) -> str:
        return self._args['identity_file']

    def cluster_definition(self) -> Dict[str, Dict[str, List[str]]]:
        raise NotImplementedError()

    def popen_hydroflow(self, bench, label: str, f: int, args: List[str]) -> proc.Proc:
        raise NotImplementedError()

    def post_benchmark(self):
        pass

    def rebuild(self, f: int, connections: Dict[str, List[str]]) -> Tuple[Dict[str, List[host.PartialEndpoint]], List[host.Endpoint]]:
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
        if config["env"]["cloud"] != "gcp":
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
        assert self._identity_file != ""
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

    def hosts(self, f: int) -> Dict[str, List[host.PartialEndpoint]]:
        hosts = {}
        for role, machines in self._machines[str(f)].items():
            hosts[role] = [host.PartialEndpoint(self._conn_cache.connect(self._internal_ip(m)), None) for m in machines]
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
        self._custom_ports = {}
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

        gcp_vpc = hydro.GCPNetwork(
            project=self._config["env"]["project"],
            existing=self._config["env"]["vpc"],
        )
        self._gcp_vpc = gcp_vpc

        last_machine = None
        custom_services = []
        for f, cluster in self._spec.items():
            self._machines[f] = {}
            for role, count in cluster.items():
                hydroflow = config["services"][role]["type"] == "hydroflow"
                if hydroflow:
                    image = config["env"]["hydroflow_image"]
                else:
                    image = config["env"]["scala_image"]
                self._machines[f][role] = []

                for i in range(count):
                    machine = deployment.GCPComputeEngineHost(
                        project=config["env"]["project"],
                        machine_type=config["env"]["machine_type"],
                        image=image,
                        region=config["env"]["zone"],
                        network=gcp_vpc,
                        user=config["env"]["user"],
                    )

                    # FIXME[Hydro CLI]: Change this once ports can be opened after a machine
                    # is started or port opening can be disabled when creating HydroflowCrate services.
                    custom_services.append(deployment.CustomService(machine, [22]))

                    self._machines[f][role].append(machine)
                    last_machine = machine
                        
        await deployment.deploy()
        await deployment.start()

        if last_machine is None:
            raise ValueError("No machines were provisioned")
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
        decomposed_label = re.match(r"([a-zA-Z0-9]+)_([0-9]+)", label)
        if decomposed_label is None:
            decomposed_label = re.match(r"([a-zA-Z0-9]+)", label)
            assert decomposed_label is not None, "The provided label was invalid. Check the comment in provision.py"
            index = 0
        else:
            index = int(decomposed_label[2])
        role = decomposed_label[1]

        assert index not in self._service_exists[role], ("The same label was provisioned twice. Are your labels of "+
            "the form f'{role}_{index}'?")
        self._service_exists[role].add(index)

        return role, index

    # It is assumed that label is of the form f"{role}_{index}"
    def popen_hydroflow(self, bench, label: str, f: int, args: List[str]) -> proc.Proc:
        """Start a new hydroflow program on a free machine.
        """
        role, index = self._parse_label(label)
        assert self._config["services"][role]["type"] == "hydroflow"

        example = self._config["services"][role]["rust_example"]
        machine = self._machines[str(f)][role][index]

        # FIXME[Hydro CLI]: Pipe stdout/stderr to files
        stdout = bench.abspath(f'{label}_out.txt')
        stderr = bench.abspath(f'{label}_err.txt')
        receiver = self._deployment.HydroflowCrate(
            src=str((Path(__file__).parent.parent / "rust").absolute()),
            example=example,
            args = args,
            on=machine,
        )
        self._services[role].append(receiver)

        return proc.HydroflowProc(receiver)

    def post_benchmark(self):
        """Stop all services and reset the state of the provisioner.

        This method should be called after every benchmark.
        """
        async def stop(_):
            for s in self._services.values():
                for service in s:
                    await service.stop()
        
        asyncio.set_event_loop(asyncio.new_event_loop())
        hydro.async_wrapper.run(stop, None)

        self.reset_services()

    def rebuild(self, f: int, connections: Dict[str, List[str]]) -> Tuple[Dict[str, List[host.PartialEndpoint]], List[host.Endpoint]]:
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
            for machine in self._machines[str(f)][role]:
                self._services[role].append(self._deployment.CustomService(machine, []))

        # Sending and receiving is with respect to the custom service
        self._custom_ports: Dict[str, Dict[str, List[hydro.CustomServicePort]]] = {"send": {}, "receive": {}}

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
                    assert sender not in self._custom_ports["receive"], f"Hyrdoflow process {sender} can only send to one custom service"
                    self._custom_ports["receive"][sender] = []
                    receivers = []
                    for s in self._services[receiver]:
                        receiver_port = s.client_port()
                        receivers.append(receiver_port)
                        # Custom ports need to be tracked separately so the final port allocations can be discovered.
                        self._custom_ports["receive"][sender].append(receiver_port)

                # Generate a list of sender ports, one for each service in the given role.
                senders = []
                if sender_hf:
                    for s in self._services[sender]:
                        sender_port = getattr(s.ports, f'send_to${receiver}${index}')
                        senders.append(sender_port)
                else:
                    assert receiver not in self._custom_ports["send"], f"Hydroflow process {receiver} can only receive from one custom service"
                    self._custom_ports["send"][receiver] = []

                    for s in self._services[sender]:
                        sender_port = s.client_port()
                        senders.append(sender_port)
                        self._custom_ports["send"][receiver].append(sender_port)

                # Connect the senders to the receivers
                demux = hydro.demux({
                    i: r for i, r in enumerate(receivers)
                })
                for i, s in enumerate(senders):
                    s.tagged(i).send_to(demux)
        
        asyncio.set_event_loop(asyncio.new_event_loop())
        hydro.async_wrapper.run(self._redeploy_and_start, None)

        # All services have been deployed so collect the endpoints that need to be returned.
        # The instance variables were populated by self._redeploy_and_start()
        endpoints: Dict[str, List[host.PartialEndpoint]] = {}
        for role, spec in self._config["services"].items():
            endpoints[role] = []
            if spec["type"] == "scala":
                for machine in self._machines[str(f)][role]:
                    endpoints[role].append(host.PartialEndpoint(
                        host=self._conn_cache.connect(self._internal_ip(machine)),
                        port=None,
                    ))
            elif spec["type"] == "hydroflow":
                if role not in self._hydroflow_endpoints:
                    for i in range(len(self._services[role])):
                        machine = self._machines[str(f)][role][i]
                        endpoints[role].append(host.PartialEndpoint(
                            host=host.FakeHost(self._internal_ip(machine)),
                            port=None,
                        ))
                    continue
                endpoints[role] = self._hydroflow_endpoints[role]
            else:
                raise ValueError(f"Unrecognized service type for role {role}: {spec['type']}")

        receive_endpoints = []
        for e in self._hydroflow_receive_endpoints.values():
            receive_endpoints.extend(e)


        # print("Endpoints:", endpoints, "\nExtra endpoints:", receive_endpoints)
        # endpoints['servers'][0] = host.PartialEndpoint(host=self._conn_cache.connect('10.138.0.32'), port=10001)
        # print("Endpoints:", endpoints, "\nExtra endpoints:", receive_endpoints)

        return endpoints, receive_endpoints

    async def _redeploy_and_start(self, _):
        """Redeploy and start the services and collect any connection endpoints for custom ports.
        """
        await self._deployment.deploy()
        await self._deployment.start()
        self._hydroflow_endpoints: Dict[str, List[host.PartialEndpoint]] = {}
        self._hydroflow_receive_endpoints: Dict[str, List[host.Endpoint]] = {}

        # Gather all the endpoints for scala --> hydroflow connections
        for receiver, ports in self._custom_ports["send"].items():
            # Sender is always a custom service and receiver is always a hydroflow service
            assert len(ports) == 1, "A scala process can currently only talk to a single hydroflow process"
            for port in ports:
                addrs = (await port.server_port()).json()['Demux']
                assert len(addrs) == len(self._services[receiver]), (f"The number of "+
                    f"addresses does not match the number of nodes with the role {receiver}.")
                assert receiver not in self._hydroflow_endpoints, (f"Multiple scala processes cannot "+
                    f"currently communicate with the same hydroflow process: {receiver}")

                self._hydroflow_endpoints[receiver] = [None]*len(addrs)
                for index, addr in addrs.items():
                    addr = addr["TcpPort"]
                    decomposed_addr = re.match(r"([0-9.]+):([0-9]+)", addr)
                    assert decomposed_addr is not None, f"Could not parse address {addr}"
                    
                    self._hydroflow_endpoints[receiver][index] = host.PartialEndpoint(
                        host.FakeHost(decomposed_addr[1]),
                        int(decomposed_addr[2]),
                    )
                # addr = (await port.server_port()).json()
                # assert  receiver not in self._hydroflow_receive_endpoints, (f"Multiple scala processes cannot "+
                #     f"currently communicate with the same hydroflow process: {sender}")
                # addr = addr["TcpPort"]
                # decomposed_addr = re.match(r"([0-9.]+):([0-9]+)", addr)
                # assert decomposed_addr is not None, f"Could not parse address {addr}"

                # self._hydroflow_endpoints[receiver] = [host.Endpoint(
                #     host.FakeHost(decomposed_addr[1]),
                #     int(decomposed_addr[2]),
                # )]

        # Gather all the endpoints for hydroflow --> scala connections
        # These are back channels on which Scala will not send messages (but hydroflow will)
        for sender, ports in self._custom_ports["receive"].items():
            # Sender is always a hydroflow service and receiver is always a custom service
            assert len(ports) == 1, "A scala process can currently only talk to a single hydroflow process"
            for port in ports:
                # FIXME[Hydro CLI]: Mux sending to clients
                print((await port.server_port()).json())
                # addrs = (await port.server_port()).json()['Tagged'] 
                # assert len(addrs) == len(self._services[sender]), (f"The number of "+
                #     f"addresses does not match the number of nodes with the role {sender}.")
                # assert sender not in self._hydroflow_receive_endpoints, (f"Multiple scala processes cannot "+
                #     f"currently communicate with the same hydroflow process: {sender}")

                # self._hydroflow_receive_endpoints[sender] = [None]*len(addrs)
                # for index, addr in addrs.items():
                #     addr = addr["TcpPort"]
                #     decomposed_addr = re.match(r"([0-9.]+):([0-9]+)", addr)
                #     assert decomposed_addr is not None, f"Could not parse address {addr}"
                    
                #     self._hydroflow_receive_endpoints[sender][index] = host.Endpoint(
                #         host.FakeHost(decomposed_addr[1]),
                #         int(decomposed_addr[2]),
                #     )
                addr = (await port.server_port()).json()['Tagged'][0]
                assert sender not in self._hydroflow_receive_endpoints, (f"Multiple scala processes cannot "+
                    f"currently communicate with the same hydroflow process: {sender}")
                addr = addr["TcpPort"]
                decomposed_addr = re.match(r"([0-9.]+):([0-9]+)", addr)
                assert decomposed_addr is not None, f"Could not parse address {addr}"

                self._hydroflow_receive_endpoints[sender] = [host.Endpoint(
                    host.FakeHost(decomposed_addr[1]),
                    int(decomposed_addr[2]),
                )]

    
    def stop(self):
        self._custom_ports = {}
        self._services = None
        self._machines = None
        self._gcp_vpc = None
        self._deployment = None