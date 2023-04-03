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

# GENERAL NOTES ABOUT PROVISIONING. Please read.
#
# Hydro CLI provisioning assumes either all services are Scala or all services
# except for the client are Hydroflow (and the client is Scala).
#
# It is assumed that label is of the form f"{role_singular}_{index}"
#
# Changes to make to individual suites:
# 1. Fork startup code to start java or hf processes, based on configuration
# 2. Call self.provisioner.rebuild() before client lag (and starting clients).
# 3. If the return value is not None, reconfigure the placement (need to write a new
#    custom_placement method) and re-write the config file before starting clients.

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

    # def popen(self, bench, label: str, f: int, cmd: str) -> proc.Proc:
        # raise NotImplementedError()

    def post_benchmark(self):
        pass

    def rebuild(self, f: int, connections: Dict[str, str]) -> Dict[str, List[host.PartialEndpoint]]:
        pass

    def stop(self):
        pass


def do(config: Dict[str, Any], spec: Dict[str, Dict[str, int]], args) -> State:
    if config["mode"] == "manual":
        raise ValueError("FIXME")
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
        self._machines: Dict[str, Dict[str, List[hydro.GCPComputeEngineHost]]] = {}
        self._identity_file = ""

        hydro.async_wrapper.run(self._configure, None)
        # Mapping from role to a set of initialized indices
        self._service_exists: Dict[str, Set[int]] = {}
        self._services: Dict[str, List[hydro.Service]] = {}
        self.reset_services()

        self._conn_cache = cluster._RemoteHostCache(self._ssh_connect)

        # FIXME[Hydro CLI]: This is a hack until we can reserve ports before deploying.
        self._next_prometheus_port = 64000
        self._prometheus_endpoints: Dict[str, List[host.Endpoint]] = {}

        # Write out the new cluster definition
        with open(args['cluster'], 'w+') as f:
            f.truncate()
            json.dump(self.cluster_definition(), f)

    def hosts(self, f: int) -> Dict[str, List[host.PartialEndpoint]]:
        hosts = {}
        for role, machines in self._machines[str(f)].items():
            hosts[role] = [host.PartialEndpoint(self._conn_cache.connect(m.internal_ip), None) for m in machines]
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
                    client.connect(address, key_filename=self._identity_file, username="rithvik")
                else:
                    client.connect(address)
                return host.RemoteHost(client)
            except:
                print(f'Attempt to connect to instance {address} failed. Retrying after delay...')
                tries += 1
                time.sleep(10)

        raise Exception(f'Failed to connect to instance {address} after 5 attempts')


    def reset_services(self):
        self._service_exists = {}
        self._services = {}
        self._next_prometheus_port = 64000
        self._prometheus_endpoints = {}
        for _, cluster in self._spec.items():
            for role, _ in cluster.items():
                self._service_exists[role] = set()
                self._services[role] = []

    async def _configure(self, _):
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
                    custom_services.append(deployment.CustomService(machine, []))

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
                    clusters[f][role].append(host.internal_ip)

        return clusters
    
    def _parse_label(self, label: str) -> Tuple[str, int]:
        decomposed_label = re.match(r"([a-zA-Z0-9]+)_([0-9]+)", label)
        if decomposed_label is None:
            decomposed_label = re.match(r"([a-zA-Z0-9]+)", label)
            assert decomposed_label is not None, "The provided label was invalid. Check the comment in provision.py"
            index = 0
        else:
            index = int(decomposed_label[2]) - 1
        role = decomposed_label[1]

        assert index not in self._service_exists[role], "The same label was provisioned twice."
        self._service_exists[role].add(index)

        return role, index

    
    # # It is assumed that label is of the form f"{role}_{index}"
    # def popen(self, bench, label: str, f: int, cmd: Callable[[int], str]) -> proc.Proc:
    #     role, index = self._parse_label(label)
    #     assert self._config["services"][role]["type"] == "scala"

    #     machine = self._machines[str(f)][role][index]
    #     svc_index = len(self._services[role]) 
    #     self._services[role].append(hydro.CustomService())

    #     def start() -> proc.Proc:
    #         machine = self._machines[str(f)][role][index]
    #         return bench.popen(
    #             host = self._conn_cache.connect(machine.internal_ip),
    #             label = label,
    #             cmd = cmd(self._new_prometheus_port())
    #         )

    #     p = proc.CustomProc(machine, start)
    #     self._custom_procs[role].append(p)

    #     if self._new_changes_deployed:
    #         p.deploy()

    #     return p

    
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
        self.reset_services()

    def rebuild(self, f: int, connections: Dict[str, List[str]]) -> Dict[str, List[host.PartialEndpoint]]:
        # Create faux services for any scala services
        for role, spec in self._config["services"].items():
            if spec["type"] != "scala":
                continue
            for machine in self._machines[str(f)][role]:
                self._services[role].append(self._deployment.CustomService(machine, []))

        self._custom_ports: Dict[str, Dict[str, List[hydro.CustomServicePort]]] = {}

        for sender, conn_receivers in connections.items():
            for receiver in conn_receivers:
                assert sender in self._services, f"Sender {sender} is not a valid role"

                sender_hf = self._config["services"][sender]["type"] == "hydroflow"
                receiver_hf = self._config["services"][receiver]["type"] == "hydroflow"
                if not sender_hf and not receiver_hf:
                    continue

                if receiver_hf:
                    receivers = [getattr(n.ports, f'receive_from${sender}').merge() for n in self._services[receiver]]
                else:
                    if receiver not in connections or sender not in connections[receiver]:
                        raise ValueError(f"A scala process that receives from a hydroflow process must also "+
                                         "send to it: {receiver} does not satisfy this criteria.")
                    receivers = []
                    for n in self._services[receiver]:
                        port = n.client_port()
                        if receiver not in self._custom_ports:
                            self._custom_ports[receiver] = {}
                        if sender not in self._custom_ports[receiver]:
                            self._custom_ports[receiver][sender] = []
                        self._custom_ports[receiver][sender].append(port)
                        receivers.append(port)

                for s in self._services[sender]:
                    if sender_hf:
                        sender_port = getattr(s.ports, f'send_to${receiver}')
                    else:
                        sender_port = s.client_port()

                    sender_port.send_to(hydro.demux({
                        i: r for i, r in enumerate(receivers)
                    }))
        
        asyncio.set_event_loop(asyncio.new_event_loop())
        hydro.async_wrapper.run(self._redeploy_and_start, None)

        endpoints: Dict[str, List[host.PartialEndpoint]] = {}
        for role, spec in self._config["services"].items():
            endpoints[role] = []
            if spec["type"] == "scala":
                for machine in self._machines[str(f)][role]:
                    endpoints[role].append(host.PartialEndpoint(
                        host=self._conn_cache.connect(machine.internal_ip),
                        port=None,
                    ))
            elif spec["type"] == "hydroflow":
                pass
            else:
                raise ValueError(f"Unrecognized service type for role {role}: {spec['type']}")

        return endpoints

    async def _redeploy_and_start(self, _):
        # FIXME[Hydro CLI]: Re-deploy
        await self._deployment.deploy()

        await self._deployment.start()

        for role, ports in self._custom_ports.items():
            print(f"\n===========\n{role}\n===========")
            for sender, ports in ports.items():
                print(f"\n{sender}")
                for port in ports:
                    print((await port.server_port()).json())
    
    def stop(self):
        self._custom_ports = {}
        self._services = None
        self._machines = None
        self._gcp_vpc = None
        self._deployment = None