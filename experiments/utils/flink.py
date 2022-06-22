import subprocess
from typing import NamedTuple

#Assumes flink redpanda-state-backend-for-flink is in flink folder. If it isn't, can set flink path manually
# flink_path = pathlib.Path(__file__).parent.parent.parent.absolute()
FLINKPATH = "/home/alec/flink-1.13.2"
ROOTPATH = "/home/alec/flink-1.13.2/redpanda-state-backend-for-flink"

class Job(NamedTuple):
    job_type: str
    proc: subprocess.CompletedProcess

#Mapping of a benchmark to its file name, edit whenever creating a new benchmark
BENCHMARK_MAP = {
    "JSON": "JSONBenchmark",
    "Wiki": "WikiBenchmark",
    "Printing": "PrintingJobBenchmark"
}

def launch_flink_producer_job(args):

    benchmark = args.benchmark
    backend = args.backend
    port = args.port
    redpanda_async = args.redpanda_async
    use_redpanda = args.use_redpanda
    checkpointing_interval = args.checkpointing_interval

    proc = subprocess.Popen([
        f"{FLINKPATH}/bin/flink",
        "run",
        "-m",
        f"localhost:{port}",
        "-c", 
        f"org.apache.flink.contrib.streaming.state.testing.{BENCHMARK_MAP[benchmark]}",
        f"{ROOTPATH}/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
        backend,
        redpanda_async, # whether to use redpanda async batching
        benchmark, # topic
        "192.168.122.132", # master machine address
        use_redpanda, # whether or not to back the backend with redpanda
        checkpointing_interval
    ], stdout=subprocess.PIPE)
   
    return Job("producer", proc)

def launch_flink_consumer_job(args):

    proc = subprocess.Popen([
        f"{FLINKPATH}/bin/flink",
        "run",
        "-m",
        f"localhost:{args.port}",
        "-c", 
        "org.apache.flink.contrib.streaming.state.query.QueryEngineFlink",
        f"{ROOTPATH}/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
        "192.168.122.132", # master machine address
        args.checkpointing_interval,
        args.num_producers
    ], stdout=subprocess.PIPE)
   
    return Job("consumer", proc)