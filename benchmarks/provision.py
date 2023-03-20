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

    def popen(self, bench, label: str, f: int, cmd: str) -> proc.Proc:
        raise NotImplementedError()

    def post_benchmark(self):
        pass

    def rebuild(self, connections: Dict[str, str], custom_services: Dict[str, int]) -> Optional[Dict[str, List[host.Endpoint]]]:
        pass

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
        self._hosts: Dict[str, Dict[str, List[hydro.GCPComputeEngineHost]]] = {}
        self._identity_file = ""

        hydro.async_wrapper.run(self._configure, None)
        # Mapping from role to a set of initialized indices
        self._service_exists: Dict[str, Set[int]] = {}
        self._services: Dict[str, List[hydro.Service]] = {}
        self._custom_procs: Dict[str, List[proc.CustomProc]] = {}
        self._new_changes_deployed = False
        self.reset_services()

        self._conn_cache = cluster._RemoteHostCache(self._ssh_connect)

        # FIXME[Hydro CLI]: This is a hack until we can reserve ports before deploying.
        self._next_prometheus_port = 64000
        self._prometheus_endpoints: Dict[str, List[host.Endpoint]] = {}

    def _new_prometheus_port(self) -> int:
        p = self._next_prometheus_port
        self._next_prometheus_port += 1
        return p

    def _ssh_connect(self, address: str) -> host.Host:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
        if self.args()['identity_file']:
            client.connect(address, key_filename=self.args()['identity_file'])
        else:
            client.connect(address)
        return host.RemoteHost(client)

    def identity_file(self) -> str:
        return self._identity_file
    
    def reset_services(self):
        self._service_exists = {}
        self._services = {}
        self._next_prometheus_port = 64000
        self._prometheus_endpoints = {}
        for _, cluster in self._spec.items():
            for role, _ in cluster.items():
                self._service_exists[role] = set()
                self._services[role] = []
                self._custom_procs[role] = []

    async def _configure(self, _):
        self._deployment = hydro.Deployment()
        deployment = self._deployment
        config = self._config

        last_machine = None
        for f, cluster in self._spec.items():
            self._hosts[f] = {}
            for role, count in cluster.items():
                hydroflow = config["services"][role]["type"] == "hydroflow"
                if hydroflow:
                    image = config["env"]["hydroflow_image"]
                else:
                    image = config["env"]["scala_image"]
                self._hosts[f][role] = []

                for i in range(count):
                    machine = deployment.GCPComputeEngineHost(
                        project=config["env"]["project"],
                        machine_type=config["env"]["machine_type"],
                        image=image,
                        region=config["env"]["zone"]
                    )
                    self._hosts[f][role].append(machine)
                    last_machine = machine
                        
        await deployment.deploy()
        print("Finished provisioning initial resources")

        if last_machine is None:
            raise ValueError("No machines were provisioned")

        self._identity_file = last_machine.ssh_key_path
    
    def cluster_definition(self) -> Dict[str, Dict[str, List[str]]]:
        clusters: Dict[str, Dict[str, List[str]]] = {}
        for f, cluster in self._hosts.items():
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

    
    # It is assumed that label is of the form f"{role}_{index}"
    def popen(self, bench, label: str, f: int, cmd: Callable[[int], str]) -> proc.Proc:
        role, index = self._parse_label(label)
        assert self._config["services"][role]["type"] == "scala"

        machine = self._hosts[str(f)][role][index]
        svc_index = len(self._services[role]) 
        self._services[role].append(hydro.CustomService())

        def start() -> proc.Proc:
            machine = self._hosts[str(f)][role][index]
            return bench.popen(
                host = self._conn_cache.connect(machine.internal_ip),
                label = label,
                cmd = cmd(self._new_prometheus_port())
            )

        p = proc.CustomProc(machine, start)
        self._custom_procs[role].append(p)

        if self._new_changes_deployed:
            p.deploy()

        return p

    
    # It is assumed that label is of the form f"{role_singular}_{index}"
    def popen_hydroflow(self, bench, label: str, f: int, args: Callable[[int], List[str]]) -> proc.Proc:
        """Start a new hydroflow program on a free machine.
        """
        role, index = self._parse_label(label)
        assert self._config["services"][role]["type"] == "hydroflow"

        example = self._config["services"][role]["rust_example"]
        machine = self._hosts[str(f)][role][index]

        # FIXME[Hydro CLI]: Pipe stdout/stderr to files
        stdout = bench.abspath(f'{label}_out.txt')
        stderr = bench.abspath(f'{label}_err.txt')
        prom_port = self._new_prometheus_port()
        receiver = self._deployment.HydroflowCrate(
            src=str((Path(__file__).parent / "rust").absolute()),
            example=example,
            args = args(prom_port),
            on=machine,
        )
        self._services[role].append(receiver)

        return proc.HydroflowProc(receiver)

    def post_benchmark(self):
        self._new_changes_deployed = False
        self.reset_service_exists()

    def rebuild(self, connections: Dict[str, str], pre_custom_service_action: Callable[[Dict[str, List[host.Endpoint]], Dict[str, List[host.Endpoint]]], None]):
        for sender, receiver in connections.items():
            if sender not in self._services:
                continue

            def portify(role: str) -> List:
                return [n.ports[role] for n in self._services[role]]

            receivers = portify(receiver)
            senders = portify(sender)

            for s in senders:
                for r in receivers:
                    s.send_to(r)

        # FIXME[Hydro CLI]: Re-deploy
        self._deployment.deploy()

        endpoints: Dict[str, List[host.Endpoint]] = {}
        if len(connections) > 0:
            # FIXME[Hydro CLI]: Determine new service endpoints
            pass
        pre_custom_service_action(endpoints, self._prometheus_endpoints)
        self._new_changes_deployed = True

        for procs in self._custom_procs.values():
            for p in procs:
                p.deploy()

        self._deployment.start()
    
    def stop(self):
        self._services = None
        self._hosts = None
        self._deployment = None