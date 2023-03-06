from typing import List, Dict, Any
import json
import subprocess

def do(args: Dict[Any, Any]):
    if 'cluster_spec' not in args or args['cluster_spec'] is None:
        return
    
    with open(args['cluster_spec'], 'r') as f:
        spec = json.load(f)

    ips = gcp_instance_group_ips('eval-workers', 'us-west1-b')

    cluster = cluster_definition(spec, ips)
    with open(args['cluster'], 'w+') as f:
        f.truncate()
        json.dump(cluster, f)

def cluster_definition(spec: Dict[str, Dict[str, int]], ips: List[str]) -> Dict[str, Dict[str, List[str]]]:
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

def gcp_instance_group_ips(group_name: str, zone: str) -> List[str]:
    # TODO: The CLI is used rather than the SDK as a temporary hack
    cmd = ['gcloud', 'compute', 'instances', 'list', '--filter', f'name~"{group_name}-.*"', '--zones', zone, '--format', 'table(networkInterfaces[].networkIP.notnull().list())']

    ips = subprocess.check_output(cmd).decode('utf8').strip().split('\n')[1:]
    return ips
