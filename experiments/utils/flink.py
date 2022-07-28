import subprocess
from typing import NamedTuple

#Assumes flink redpanda-state-backend-for-flink is in flink folder. If it isn't, can set flink path manually
BASE = "/local" # /home/alec
FLINKPATH = f"{BASE}/flink-1.13.2"
ROOTPATH = f"{BASE}/flink-1.13.2/redpanda-state-backend-for-flink"

class Job(NamedTuple):
    job_type: str
    proc: subprocess.CompletedProcess

#Mapping of a benchmark to its file name, edit whenever creating a new benchmark
BENCHMARK_MAP = {
    "Wiki": "WikiSink",
     # "JSON": "JSONBenchmark",
    # "Printing": "PrintingJobBenchmark"
}

def launch_flink_producer_job(args):

    proc = subprocess.Popen([
        f"{FLINKPATH}/bin/flink",
        "run",
        "-m",
        f"localhost:{args.port}",
        "-c", 
        f"input.{BENCHMARK_MAP[args.benchmark]}",
        f"{ROOTPATH}/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
        args.benchmark, # topic
        "192.168.122.132", # master machine address
    ], stdout=subprocess.PIPE)
   
    return Job("producer", proc)

def launch_flink_consumer_job(args):

    proc = subprocess.Popen([
        f"{FLINKPATH}/bin/flink",
        "run",
        "-m",
        f"localhost:{args.port}",
        "-c", 
        "etl.QueryEngineFlink",
        f"{ROOTPATH}/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
        args.master, # master machine address
        args.checkpointing_interval,
        str(args.producers)
    ], stdout=subprocess.PIPE)
   
    return Job("consumer", proc)