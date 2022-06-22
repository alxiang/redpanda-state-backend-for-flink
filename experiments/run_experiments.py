import argparse
import datetime
from datetime import timezone, date
import json
import time
import os

from utils import redpanda, k8s
from utils.flink import BENCHMARK_MAP, ROOTPATH, launch_flink_producer_job, launch_flink_consumer_job


today_folder = date.today().strftime("%m-%d-%Y")

CONSUMER_ONLY = True

def run_experiment_trials(args):

    k = args.k
    benchmark = args.benchmark
    backend = args.backend
    redpanda_async = args.redpanda_async
    producers = args.producers
    consumers = args.consumers

    current_time = datetime.datetime.now().strftime("%m-%d-%Y-%H:%M:%S")
    filename = f"{ROOTPATH}/experiments/{today_folder}/{args.checkpointing_interval}_{args.producers}x{args.consumers}_{current_time}_{backend}_{benchmark}.json"
    print(f"[{current_time}]: Running experiment {backend} with {benchmark} benchmark")
    with open(filename, mode='w+') as file:
        result = []

        if(CONSUMER_ONLY):
            jobs = []
            for i in range(producers):
                print(f"Submitting Producer Job {i}")
                jobs.append(launch_flink_producer_job(args))
                time.sleep(1)

            for job in jobs:
                while job.proc.poll() is None:
                    time.sleep(0.1)
            print("Populated data for the consumers to consume")


        for i in range(k):
            print(f"Starting Trial {i}")

            k8s.reset_kube_cluster()
            
            # if(backend == "redpanda" and redpanda_async == "true" and k > 1):
            #     time.sleep(5) # give time for the prev thread to timeout
            
            # Get kube pods and start time to check the logs later
            pods = k8s.get_kube_pods()
            start_time = datetime.datetime.now(timezone.utc).astimezone().isoformat()

            # clear the redpanda topic (if using redpanda backend)
            if(not CONSUMER_ONLY and backend == "redpanda"):
                redpanda.delete_topic(benchmark)
                redpanda.create_topic(benchmark)

            jobs = []

            if(not CONSUMER_ONLY):
                for i in range(producers):
                    print(f"Submitting Producer Job {i}")
                    jobs.append(launch_flink_producer_job(args))
                    time.sleep(1) # slightly stagger job submission so no slot errors

            for i in range(consumers):
                print(f"Submitting Consumer Job {i}")
                jobs.append(launch_flink_consumer_job(args))
                time.sleep(1) # slightly stagger job submission so no slot errors
            

            for job in jobs:
                while job.proc.poll() is None:
                    time.sleep(1) # block until the job is ready for collection
                    
                text_output = job.proc.stdout.read().decode("utf-8")
            
                runtime_tag = "Job Runtime: "

                start_ind = text_output.rfind(runtime_tag)
                if start_ind == -1: 
                    print("Trial resulted in error")
                    print(text_output)

                    result.append({
                        "trial": i, 
                        "time": "ERROR", 
                        "type": job.job_type, 
                        "backend": backend, 
                        "benchmark": benchmark,
                        "redpanda_async": redpanda_async
                    })

                else:
                    start_ind = start_ind + len(runtime_tag)
                    # Not sure if this is safe to get the time if it isn't always in ms
                    end_ind = start_ind+text_output[start_ind:].find("ms") 
                    time_taken = int(text_output[start_ind:end_ind])

                    print(f"{job.job_type} job (Trial {i}) finished in {time_taken} ms")
                    result.append({
                        "trial": i, 
                        "time": time_taken, 
                        "type": job.job_type, 
                        "backend": backend, 
                        "benchmark": benchmark,
                        "redpanda_async": redpanda_async,
                        "checkpointing_interval": args.checkpointing_interval
                    })

            # get the latency from kubernetes logs
            if(backend == "redpanda"):
                tags = ["[SNAPSHOT_TIME]", "[FLINK_QUESTDB_RUNTIME]"]
                logged_tag_values = k8s.get_tags_from_pod_logs(
                    pods, 
                    start_time, 
                    tags
                )

                res_dict = {
                    "trial": i,
                    "backend": backend, 
                    "benchmark": benchmark,
                    "redpanda_async": redpanda_async,
                }

                for tag in tags:
                    res_dict[tag] = logged_tag_values[tag]

                result.append(res_dict)
            

        json.dump(result, file, indent=4)
        file.flush()
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('k', type=int, default=1, nargs='?') # number of runs
    parser.add_argument('checkpointing_interval', type=str, default="1000", nargs='?')
    parser.add_argument('benchmark', type=str, default='Wiki', nargs='?')

    parser.add_argument('producers', type=int, default=1, nargs='?')
    parser.add_argument('consumers', type=int, default=1, nargs='?')

    # to implement
    parser.add_argument('redpanda_async', type=str, default='true', nargs='?')

    # deprecated
    parser.add_argument('backend', type=str, default='redpanda', nargs='?')
    parser.add_argument('use_redpanda', type=str, default='true', nargs='?')

    parser.add_argument('port', type=str, default="8888", nargs='?')
    args = parser.parse_args()

    # make a folder to save the experiments for today
    os.makedirs(today_folder, exist_ok=True)

    if args.benchmark not in BENCHMARK_MAP:
        print("Can't find benchmark with name", args.benchmark)
        return

    run_experiment_trials(args)


if __name__ ==  "__main__":
    main()
