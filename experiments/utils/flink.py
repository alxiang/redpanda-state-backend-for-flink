import subprocess

from utils.jobs import Job

#Assumes flink redpanda-state-backend-for-flink is in flink folder. If it isn't, can set flink path manually
BASE = "/local" # /home/alec
HOME = "/users/alxiang"
FLINKPATH = f"{BASE}/flink-1.13.2"
ROOTPATH = f"{BASE}/flink-1.13.2/redpanda-state-backend-for-flink"

#Mapping of a benchmark to its file name, edit whenever creating a new benchmark
BENCHMARK_MAP = {
    "Wiki": "WikiSink",
    "VectorSimDelta": "KafkaToDelta"
     # "JSON": "JSONBenchmark",
    # "Printing": "PrintingJobBenchmark"
}

def launch_flink_producer_job(args) -> Job:

    if args.benchmark == "VectorSimDelta":
         proc = subprocess.Popen([
            "python3.8",
            "/local/flink-1.13.2/redpanda-state-backend-for-flink/src/python/applications/vector_similarity_search/kafka_datasource.py",
            args.master,
            str(100_000), # number of vectors to produce into topic
            str(64) # length of each vector
        ], stdout=subprocess.PIPE)

    proc = subprocess.Popen([
        f"{FLINKPATH}/bin/flink",
        "run",
        "-m",
        f"localhost:{args.port}",
        "-c", 
        f"input.KafkaToDelta",
        f"{ROOTPATH}/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
        args.master, # master machine address
    ], stdout=subprocess.PIPE)
   
    return Job("producer", proc)

def launch_flink_consumer_job(args) -> Job:

    if args.application == "VectorSimKafka":
        etl_class = "etl.KafkaToQuest"
    elif args.application == "VectorSimDelta":
        etl_class = "etl.DeltaToQuest"
        
    proc = subprocess.Popen([
        f"{FLINKPATH}/bin/flink",
        "run",
        "-m",
        f"localhost:{args.port}",
        "-c", 
        etl_class,
        f"{ROOTPATH}/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
        args.master, # master machine address
        args.checkpointing_interval,
        args.application,
        str(args.producers)
    ], stdout=subprocess.PIPE)
   
    return Job("consumer", proc)