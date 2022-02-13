import pathlib
import argparse
import datetime;
import json
import subprocess
import sys

root_path = pathlib.Path(__file__).parent.parent.absolute()

#Assumes flink redpanda-state-backend-for-flink is in flink folder. If it isn't, can set flink path manually
flink_path = pathlib.Path(__file__).parent.parent.parent.absolute()

#Mapping of a benchmark to its file name, edit whenever creating a new benchmark
benchmark_map = {
    "JSON": "JSONBenchmark",
    "wiki": "WikiBenchmark"
}

def run_experiment_trials(k, benchmark, backend):
    current_time = datetime.datetime.now().strftime("%m-%d-%Y-%H:%M:%S")
    file_name = f"{root_path}/experiments/{current_time}_{backend}_{benchmark}.json"
    print(f"[{current_time}]: Running experiment {backend} with {benchmark} benchmark")
    with open(file_name, mode='w+') as file:
        result = []
        for i in range(1, k+1):
            print(f"Starting Trial {i}")
            output = subprocess.run([
                f"{flink_path}/bin/flink", 
                "run",
                "-c", 
                f"org.apache.flink.contrib.streaming.state.testing.{benchmark_map[benchmark]}",
                f"{root_path}/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
                backend 
            ], capture_output=True)

            text_output = output.stdout.decode(sys.stdout.encoding)
            start_location = text_output.find("Job Runtime: ")+len("Job Runtime: ")
            if start_location < len("Job Runtime: "):
                print("Trial resulted in error")
                result.append({"trial": i, "time": "ERROR"})
            else:
                end_location = start_location+text_output[start_location:].find("ms") #Not sure if this is safe to get the time if it isn't always in ms
                time_taken = int(text_output[start_location:end_location])
                print(f"Trial {i} finished with time {time_taken} ms")
                result.append({"trial": i, "time": time_taken})

        json.dump(result, file, indent=4)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('k', type=int)
    parser.add_argument('benchmark')
    parser.add_argument('backend', nargs='?', default="")
    args = parser.parse_args()

    if args.benchmark not in benchmark_map:
        print("Can't find benchmark with name", args.benchmark)
        return

    if args.backend:
        run_experiment_trials(args.k, args.benchmark, args.backend)
    else:
        run_experiment_trials(args.k, args.benchmark, "hashmap")
        run_experiment_trials(args.k, args.benchmark, "redpanda")
        run_experiment_trials(args.k, args.benchmark, "rocksdb")

if __name__ ==  "__main__":
    main()
