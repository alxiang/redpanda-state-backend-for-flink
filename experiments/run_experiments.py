import pathlib
import argparse
import datetime
from datetime import timezone, date
import json
import subprocess
import sys
import time
import kubernetes.client
from kubernetes import client, config
import os

root_path = "/home/alec/flink-1.13.2/redpanda-state-backend-for-flink"# pathlib.Path(__file__).parent.parent.absolute()
print(root_path)

#Assumes flink redpanda-state-backend-for-flink is in flink folder. If it isn't, can set flink path manually
# flink_path = pathlib.Path(__file__).parent.parent.parent.absolute()
flink_path = "/home/alec/flink-1.13.2"

#Mapping of a benchmark to its file name, edit whenever creating a new benchmark
benchmark_map = {
    "JSON": "JSONBenchmark",
    "Wiki": "WikiBenchmark",
    "Printing": "PrintingJobBenchmark"
}

today_folder = date.today().strftime("%m-%d-%Y")

def launch_flink_job(args, flink_path, root_path):

    benchmark = args.benchmark
    backend = args.backend
    port = args.port
    redpanda_async = args.redpanda_async
    use_redpanda = args.use_redpanda
    checkpointing_interval = args.checkpointing_interval

    proc = subprocess.Popen([
        f"{flink_path}/bin/flink",
        "run",
        "-m",
        f"localhost:{port}",
        "-c", 
        f"org.apache.flink.contrib.streaming.state.testing.{benchmark_map[benchmark]}",
        f"{root_path}/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
        backend,
        redpanda_async, # whether to use redpanda async batching
        benchmark, # topic
        "192.168.122.132", # master machine address
        use_redpanda, # whether or not to back the backend with redpanda
        checkpointing_interval
    ], stdout=subprocess.PIPE)
   
    return proc

def launch_flink_query_job(args, flink_path, root_path):

    benchmark = args.benchmark
    backend = args.backend
    port = args.port
    redpanda_async = args.redpanda_async
    use_redpanda = args.use_redpanda
    checkpointing_interval = args.checkpointing_interval

    proc = subprocess.Popen([
        f"{flink_path}/bin/flink",
        "run",
        "-m",
        f"localhost:{port}",
        "-c", 
        f"org.apache.flink.contrib.streaming.state.query.QueryEngineFlink",
        f"{root_path}/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
        "192.168.122.132", # master machine address
    ], stdout=subprocess.PIPE)
   
    return proc


def run_experiment_trials(args):

    k = args.k
    benchmark = args.benchmark
    backend = args.backend
    port = args.port
    redpanda_async = args.redpanda_async
    jobs = args.jobs

    current_time = datetime.datetime.now().strftime("%m-%d-%Y-%H:%M:%S")
    file_name = f"{root_path}/experiments/{today_folder}/{current_time}_{backend}_{benchmark}.json"
    print(f"[{current_time}]: Running experiment {backend} with {benchmark} benchmark")
    with open(file_name, mode='w+') as file:
        result = []
        for i in range(k):
            print(f"Starting Trial {i}")
            
            if(backend == "redpanda" and redpanda_async == "true" and k > 1):
                time.sleep(5) # give time for the prev thread to timeout
            

            # pods = reset_kube_cluster(args)
            pods = get_kube_pods()
            start_time = datetime.datetime.now(timezone.utc).astimezone().isoformat()
            print(start_time)

            # clear the redpanda topic (if using redpanda backend)
            if(backend == "redpanda"):

                print(f"cleaning redpanda topic: {benchmark}")
                
                output = subprocess.run([
                    'rpk',
                    'topic',
                    'delete',
                    benchmark
                ], capture_output=True)

                print(f"resetting redpanda topic: {benchmark}")
                
                output = subprocess.run([
                    'rpk',
                    'topic',
                    'create',
                    benchmark,
                    '-c',
                    'message.timestamp.type=LogAppendTime'
                ], capture_output=True)

                print(f"cleaning redpanda topic: {benchmark+'Offsets'}")

                output = subprocess.run([
                    'rpk',
                    'topic',
                    'delete',
                    benchmark + "Offsets"
                ], capture_output=True)

                print(f"cleaning redpanda topic: {benchmark+'Checkpoint'}")

                output = subprocess.run([
                    'rpk',
                    'topic',
                    'delete',
                    benchmark + "Checkpoint"
                ], capture_output=True)
                
                # assert(output.returncode == 0)

            procs = []
            for t in range(jobs):
                print(f"Submitting Job {t}")
                # procs.append(launch_flink_job(args, flink_path, root_path))
                time.sleep(1) # slightly stagger job submission so no slot errors
            procs.append(launch_flink_query_job(args, flink_path, root_path))

            for t, proc in enumerate(procs):
                while proc.poll() is None:
                    time.sleep(1) # block until the job is ready for collection
                    
                text_output = proc.stdout.read().decode("utf-8")
            
                start_location = text_output.find("Job Runtime: ")+len("Job Runtime: ")
                if start_location < len("Job Runtime: "):
                    print("Trial resulted in error")
                    result.append({
                        "trial": i, 
                        "time": "ERROR", 
                        "job": t, 
                        "backend": backend, 
                        "benchmark": benchmark,
                        "redpanda_async": redpanda_async
                    })

                    print(text_output)
                else:
                    end_location = start_location+text_output[start_location:].find("ms") #Not sure if this is safe to get the time if it isn't always in ms
                    time_taken = int(text_output[start_location:end_location])
                    print(f"Job {t} (Trial {i}) finished with time {time_taken} ms")
                    result.append({
                        "trial": i, 
                        "time": time_taken, 
                        "job": t,
                        "backend": backend, 
                        "benchmark": benchmark,
                        "redpanda_async": redpanda_async
                    })

            # get the latency from kubernetes logs
            if(backend == "redpanda"):
                latencies, stdevs, checkpoint_times = get_latencies_from_pod_logs(pods, start_time)
                result.append({
                    "trial": i,
                    "backend": backend, 
                    "benchmark": benchmark,
                    "redpanda_async": redpanda_async,
                    "latencies": latencies,
                    "latency_stdevs": stdevs,
                    "checkpoint_times": checkpoint_times
                })
            

        json.dump(result, file, indent=4)
        file.flush()

def get_kube_pods():

    config.load_kube_config("/home/alec/.kube/config")

    v1 = kubernetes.client.CoreV1Api()
    # print("Listing task executor pods with their IPs:")

    pods = []
    ret = v1.list_pod_for_all_namespaces(watch=False)
    for i in ret.items:
        if(i.metadata.name.find("flink-taskmanager") != -1 and i.status.pod_ip is not None):
            # print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
            pods.append(i.metadata.name)

    return pods

def get_latencies_from_pod_logs(pods, start_time):

    logs = []

    for pod in pods:

        output = subprocess.run([
            'kubectl',
            'logs',
            f'--since-time={start_time}',
            pod
        ], capture_output=True)
        logs.append(output.stdout.decode("utf-8"))

    # print(logs)

    res1 = []
    res2 = []
    res3 = []
    for log in logs:
        latencies = []
        stdevs = []
        checkpoint_times = []
        log_as_list = log.split("\n")
        for x in log_as_list:
            print(x)
            if(x.find("[LATENCY]") != -1):
                _, latency = x.split(" ")
                latencies.append(float(latency))
            if(x.find("[LATENCY_STDEV]") != -1):
                _, stdev = x.split(" ")
                stdevs.append(float(stdev))
            if(x.find("[CHECKPOINT]") != -1):
                _, checkpoint_time = x.split(" ")
                checkpoint_times.append(float(checkpoint_time))


        if(len(latencies) > 0):
            res1.append(latencies)
        if(len(stdevs) > 0):
            res2.append(stdevs)
        if(len(checkpoint_times) > 0):
            res3.append(checkpoint_times)

    return res1, res2, res3

def reset_kube_cluster(args):
    print("Reseting the kube cluster")
    output = subprocess.run([
            'kubectl',
            'delete',
            '-f',
            f'{root_path}/flink-kubernetes/taskmanager-session-deployment.yaml'
        ], capture_output=True)

    time.sleep(3)

    output = subprocess.run([
            'kubectl',
            'create',
            '-f',
            f'{root_path}/flink-kubernetes/taskmanager-session-deployment.yaml'
        ], capture_output=True)

    while(len(get_kube_pods()) < args.jobs):
        # print(len(get_kube_pods()), args.jobs)
        time.sleep(1)


    print("Cluster has been reset")
    return get_kube_pods()
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('k', type=int, default=1, nargs='?')
    parser.add_argument('benchmark', type=str, default='Wiki', nargs='?')
    parser.add_argument('backend', type=str, default='redpanda', nargs='?')
    parser.add_argument('redpanda_async', type=str, default='true', nargs='?')
    parser.add_argument('jobs', type=int, default=1, nargs='?')
    parser.add_argument('use_redpanda', type=str, default='true', nargs='?')
    parser.add_argument('checkpointing_interval', type=str, default="10", nargs='?')
    parser.add_argument('port', type=str, default="8888", nargs='?')
    args = parser.parse_args()

    # pods = get_kube_pods()
    # print(pods)

    # make a folder to save the experiments for today
    try:
        os.makedirs(today_folder, exist_ok=True)
    except:
        pass

    if args.benchmark not in benchmark_map:
        print("Can't find benchmark with name", args.benchmark)
        return

    if args.backend != "all":
        run_experiment_trials(args)
    else:
        args.backend = "redpanda"
        args.redpanda_async = "true"
        run_experiment_trials(args)

        # args.backend = "redpanda"
        # args.redpanda_async = "false"
        # run_experiment_trials(args)

        args.backend = "rocksdb"
        run_experiment_trials(args)

        args.backend = "hashmap"
        run_experiment_trials(args)

if __name__ ==  "__main__":
    main()
