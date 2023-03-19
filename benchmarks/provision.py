from typing import List, Dict, Any, Tuple, NamedTuple, Set, Optional
import json
import subprocess
import hydro
import hydro.async_wrapper
from pathlib import Path
import re
from . import proc
from . import host

# GENERAL NOTES ABOUT PROVISIONING. Please read.
#
# All "roles" should be plural, and the corresponding label of the serivce should be
# f"{role_singular}_{index}". For example, the role "workers" should have label, "worker_1",
# "worker_2", ..... If no index exists, then it is assumed to be 0.
#
# Hydro CLI provisioning assumes either all services are Scala or all services
# except for the client are Hydroflow (and the client is Scala).
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
        self._hf_service_exists: Dict[str, Set[int]] = {}
        self._services: Dict[str, List[hydro.Service]] = {}
        self.reset_services()

    def identity_file(self) -> str:
        return self._identity_file
    
    def reset_services(self):
        self._hf_service_exists = {}
        self._services = {}
        for _, cluster in self._spec.items():
            for role, _ in cluster.items():
                self._hf_service_exists[role] = set()
                self._services[role] = []

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
    
    # It is assumed that label is of the form f"{role_singular}_{index}", where role_singular
    # is the singular form of the role
    def popen_hydroflow(self, bench, label: str, f: int, args: List[str]) -> proc.Proc:
        """Start a new hydroflow program on a free machine.
        """
        decomposed_label = re.match(r"([a-zA-Z0-9]+)_([0-9]+)", label)
        if decomposed_label is None:
            decomposed_label = re.match(r"([a-zA-Z0-9]+)", label)
            assert decomposed_label is not None, "The provided label was invalid. Check the comment in provision.py"
            index = 0
        else:
            index = int(decomposed_label[2]) - 1
        role = decomposed_label[1] + "s"

        assert index not in self._hf_service_exists[role], "The same label was provisioned twice."
        assert self._config["services"][role]["type"] == "hydroflow"

        example = self._config["services"][role]["rust_example"]
        host = self._hosts[str(f)][role][index]

        # FIXME[Hydro CLI]: Pipe stdout/stderr to files
        stdout = bench.abspath(f'{label}_out.txt')
        stderr = bench.abspath(f'{label}_err.txt')
        receiver = self._deployment.HydroflowCrate(
            src=str((Path(__file__).parent / "rust").absolute()),
            example=example,
            args = args,
            on=host,
        )
        self._hf_service_exists[role].add(index)
        self._services[role].append(receiver)

        return proc.HydroProc(receiver)

    def post_benchmark(self):
        self.reset_service_exists()

    def rebuild(self, connections: Dict[str, str], custom_services: Dict[str, int]) -> Optional[Dict[str, List[host.Endpoint]]]:
        for role, count in custom_services.items():
            self._services[role] = []
            for i in range(count):
                self._services[role].append(hydro.CustomService())

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
        self._deployment.start()

        if len(connections) == 0:
            return None
        
        # FIXME[Hydro CLI]: Determine and return new service endpoints
        return None

    
    def stop(self):
        self._hosts = None
        self._deployment = None 