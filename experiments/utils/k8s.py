import subprocess
from collections import defaultdict
from typing import Dict, List
from utils.flink import HOME

from kubernetes import client, config

def get_kube_pods() -> List[str]:

    config.load_kube_config(f"{HOME}/.kube/config")

    v1 = client.CoreV1Api()

    pods = []
    ret = v1.list_pod_for_all_namespaces(watch=False)
    for i in ret.items:
        if(i.metadata.name.find("flink-taskmanager") != -1 and i.status.pod_ip is not None):
            pods.append(i.metadata.name)

    return pods

def get_tags_from_pod_logs(pods, start_time, tags):

    logs = []

    for pod in pods:

        output = subprocess.run([
            'kubectl',
            'logs',
            f'--since-time={start_time}',
            pod
        ], capture_output=True)
        logs.append(output.stdout.decode("utf-8"))


    res = defaultdict(list) # map string to list of lists

    for log in logs:
        res_nested = defaultdict(list) # map string to list

        log_as_list = log.split("\n")
        for log_line in log_as_list:

            for tag in tags:
                if(log_line.find(tag) != -1):
                    _, tag_val = log_line.split(" ")
                    res_nested[tag].append(float(tag_val))

        for tag in tags:
            if(len(res_nested[tag]) > 0):
                res[tag].append(res_nested[tag])

    return res

def reset_kube_cluster() -> None:
    print("Resetting the kube cluster")

    for task_executor in get_kube_pods():
        output = subprocess.run([
                'kubectl',
                'exec',
                task_executor,
                '--',
                'rm',
                '-r',
                '.questdb/db/wikitable'
            ], capture_output=True)

        output = subprocess.run([
                'kubectl',
                'exec',
                task_executor,
                '--',
                'rm',
                '-r',
                '.questdb/db/vectortable'
            ], capture_output=True)

        output = subprocess.run([
                'kubectl',
                'exec',
                task_executor,
                '--',
                'rm',
                '-r',
                '.questdb/db/wikitable.lock'
            ], capture_output=True)

        output = subprocess.run([
                'kubectl',
                'exec',
                task_executor,
                '--',
                'rm',
                '-r',
                '.questdb/db/vectortable.lock'
            ], capture_output=True)

        # output = subprocess.run([
        #         'kubectl',
        #         'exec',
        #         task_executor,
        #         '--',
        #         'watch',
        #         '-n',
        #         '1',
        #         "'chmod -R 777 /opt/flink/.questdb && chown -R flink:flink /opt/flink/.questdb'",
        #         '&'
        #     ], capture_output=True)

        # print(task_executor, output)

    print("Cluster has been reset")
