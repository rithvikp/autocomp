from typing import List, Dict, Any, Tuple
import json
import subprocess
import hydro
import hydro.async_wrapper

class State:
    def __init__(self, config: Dict[str, Any], spec: Dict[str, Dict[str, int]]):
        self._config = config
        self._spec = spec

    def cluster_definition(self) -> Dict[str, Dict[str, List[str]]]:
        raise NotImplementedError()
    
    def stop(self):
        pass

def do(config: Dict[str, Any], spec: Dict[str, Dict[str, int]], args) -> State:
    if config["mode"] == "manual":
        if config["env"]["cloud"] == "gcp":
            state = ManualState(config, spec)
        else:
            raise ValueError(f'Unsupported cloud: {config["cloud"]}')
    elif config["mode"] == "hydro":
        state = HydroState(config, spec)
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
    def __init__(self, config: Dict[str, Any], spec: Dict[str, Dict[str, int]]):
        super().__init__(config, spec)
        self._ips = gcp_instance_group_ips(config["manual"]["instance_group_name"], config["env"]["zone"])

    def cluster_definition(self) -> Dict[str, Dict[str, List[str]]]:
        return cluster_definition_from_ips(self._spec, self._ips)

class HydroState(State):
    def __init__(self, config: Dict[str, Any], spec: Dict[str, Dict[str, int]]):
        super().__init__(config, spec)
        if config["env"]["cloud"] != "gcp":
            raise ValueError(f'Unsupported cloud for Hydro CLI deployments: {config["env"]["cloud"]}')
        self._hosts: Dict[str, Dict[str, List[hydro.GCPComputeEngineHost]]] = {}

        hydro.async_wrapper.run(self._configure, None)

    async def _configure(self, _):
        self._deployment = hydro.Deployment()
        deployment = self._deployment
        config = self._config

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
                        
        await deployment.deploy()
        print("Finished provisioning resources")

        await deployment.start()
    
    def cluster_definition(self) -> Dict[str, Dict[str, List[str]]]:
        clusters = {}
        for f, cluster in self._hosts.items():
            clusters[f] = {}
            for role, hosts in cluster.items():
                clusters[f][role] = []
                for host in hosts:
                    clusters[f][role].append(host.internal_ip)

        return clusters
    
    def stop(self):
        self._hosts = None
        self._deployment = None