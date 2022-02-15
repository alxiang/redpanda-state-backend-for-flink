import pathlib
import argparse
import datetime;
import json
import subprocess
import sys

root_path = "/home/alec/flink-1.13.2/redpanda-state-backend-for-flink"# pathlib.Path(__file__).parent.parent.absolute()
print(root_path)

#Assumes flink redpanda-state-backend-for-flink is in flink folder. If it isn't, can set flink path manually
# flink_path = pathlib.Path(__file__).parent.parent.parent.absolute()
flink_path = "/home/alec/flink-1.13.2"
print()

#Mapping of a benchmark to its file name, edit whenever creating a new benchmark
benchmark_map = {
    "JSON": "JSONBenchmark",
    "Wiki": "WikiBenchmark"
}

def launch_flink_job(args, flink_path, root_path):

    benchmark = args.benchmark
    backend = args.backend
    port = args.port
    redpanda_async = args.redpanda_async

    proc = subprocess.Popen([
        f"{flink_path}/bin/flink",
        "run",
        "-m",
        f"localhost:{port}",
        "-c", 
        f"org.apache.flink.contrib.streaming.state.testing.{benchmark_map[benchmark]}",
        f"{root_path}/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
        backend,
        "true", # whether to use redpanda async batching
        benchmark, # topic
        "192.168.122.131" # master machine address
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
    file_name = f"{root_path}/experiments/{current_time}_{backend}_{benchmark}.json"
    print(f"[{current_time}]: Running experiment {backend} with {benchmark} benchmark")
    with open(file_name, mode='w+') as file:
        result = []
        for i in range(k):
            print(f"Starting Trial {i}")
            procs = []
            for t in range(jobs):
                print(f"Submitting Job {t}")
                procs.append(launch_flink_job(args, flink_path, root_path))


            # output = subprocess.run([
            #     f"{flink_path}/bin/flink",
            #     "run",
            #     "-m",
            #     f"localhost:{port}",
            #     "-c", 
            #     f"org.apache.flink.contrib.streaming.state.testing.{benchmark_map[benchmark]}",
            #     f"{root_path}/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
            #     backend,
            #     "true", # whether to use redpanda async batching
            #     benchmark, # topic
            #     "192.168.122.131" # master machine address
            # ], capture_output=True)

            for t, proc in enumerate(procs):
                while proc.poll() is None:
                    sleep(1)
                    pass
                    
                text_output = proc.stdout.read()
            
                start_location = text_output.find("Job Runtime: ")+len("Job Runtime: ")
                if start_location < len("Job Runtime: "):
                    print("Trial resulted in error")
                    result.append({"trial": i, "time": "ERROR", "job": t})

                    print(text_output)
                else:
                    end_location = start_location+text_output[start_location:].find("ms") #Not sure if this is safe to get the time if it isn't always in ms
                    time_taken = int(text_output[start_location:end_location])
                    print(f"Trial {i} finished with time {time_taken} ms")
                    result.append({"trial": i, "time": time_taken, "job": t})

        json.dump(result, file, indent=4)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('k', type=int)
    parser.add_argument('benchmark')
    parser.add_argument('backend', nargs='?', default="")
    parser.add_argument('port', type=str)
    parser.add_argument('redpanda_async', type=str, default='true')
    parser.add_argument('jobs', type=int, default=1)
    args = parser.parse_args()

    if args.benchmark not in benchmark_map:
        print("Can't find benchmark with name", args.benchmark)
        return

    if args.backend:
        run_experiment_trials(args)
    else:
        args.benchmark = "redpanda"
        args.redpanda_async = "true"
        run_experiment_trials(args)

        args.benchmark = "redpanda"
        args.redpanda_async = "false"
        run_experiment_trials(args)

        args.benchmark = "rocksdb"
        run_experiment_trials(args)

        args.benchmark = "hashmap"
        run_experiment_trials(args)

if __name__ ==  "__main__":
    main()
