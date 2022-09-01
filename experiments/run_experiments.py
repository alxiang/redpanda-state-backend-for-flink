import argparse
import datetime
from datetime import timezone, date
import json
import subprocess
import time
import os
from utils import apps, flink, k8s, redpanda
from utils.jobs import wait_for_jobs, Job

today_folder = date.today().strftime("%m-%d-%Y")


def run_experiment_trials(args) -> None:

    k = args.k
    benchmark = args.benchmark
    producers = args.producers
    consumers = args.consumers
    application = args.application

    current_time = datetime.datetime.now().strftime("%m-%d-%Y-%H:%M:%S")
    filename = f"{flink.ROOTPATH}/experiments/{today_folder}/{args.checkpointing_interval}_{args.producers}x{args.consumers}_{current_time}_{benchmark}.json"
    print(f"[{current_time}]: Running experiment {producers}x{consumers} with {benchmark} benchmark, {k} trials")
    with open(filename, mode='w+') as file:
        result = []

        # redpanda.delete_topic(benchmark)
        # redpanda.create_topic(benchmark)
        if application == "VectorSimKafka":
            # Clear the topic
            try:
                print("Cleaning the Vector topic if it exists")
                proc = subprocess.Popen([
                    "/local/flink-1.13.2/redpanda-state-backend-for-flink/kafka/kafka_2.13-3.2.1/bin/kafka-topics.sh",
                    "--delete",
                    "--topic",
                    "Vector",
                    "--bootstrap-server",
                    f"{args.master}:9192"
                ], stdout=subprocess.PIPE)

                wait_for_jobs(Job("cleaning", proc))
            except:
                pass
        elif application == "VectorSimDelta":
            pass # TODO: cleanup for delta lake
                    

        print("Populated data for the consumers to consume")

        for i in range(k):
            print(f"Starting Trial {i}")

            ### INITIALIZATION
            k8s.reset_kube_cluster()

            # Get kube pods and start time to check the logs later
            pods = k8s.get_kube_pods()
            start_time = datetime.datetime.now(timezone.utc).astimezone().isoformat()


            ### JOB SUBMISSION
            jobs = []
            
            # Launch the data source on each pod
            # TODO: these should run unbounded, and 
            # we should determine steady state throughput and data freshness after warmup (everything is running together)
            if application == "QuestDBClient":
                for i in range(producers):
                    print(f"Submitting Producer Job {i}")
                    jobs.append(flink.launch_flink_producer_job(args))
                    time.sleep(1)
            elif application == "VectorSimKafka": 
                proc = subprocess.Popen([
                    "python3.8",
                    "/local/flink-1.13.2/redpanda-state-backend-for-flink/src/python/applications/vector_similarity_search/kafka_datasource.py",
                    args.master,
                    str(100_000*producers), # number of vectors to produce into topic
                    str(64) # length of each vector
                ], stdout=subprocess.PIPE)
            # elif application == "VectorSimDelta":
            #     proc = subprocess.Popen([
            #         "python3.8",
            #         "/local/flink-1.13.2/redpanda-state-backend-for-flink/src/python/applications/vector_similarity_search/delta_datasource.py",
            #         args.master,
            #         str(100_000*producers), # number of vectors to produce into topic
            #         str(64) # length of each vector
            #     ], stdout=subprocess.PIPE)
            # jobs.append(Job("producer", proc))

            # Launch Flink consumers to be scheduled across the cluster by Flink
            for i in range(consumers):
                print(f"Submitting Consumer Job {i}")
                jobs.append(flink.launch_flink_consumer_job(args))
                # slightly stagger job submission so no slot errors
                time.sleep(1)

            # Launch the application on each pod
            for pod in k8s.get_kube_pods(): 
                print(f"Submitting application job {i} for application {args.application}")
                jobs.append(apps.launch_application_job(args, pod))
            wait_for_jobs(jobs)


            ### METRICS COLLECTION
            for job in jobs:
                text_output = job.proc.stdout.read().decode("utf-8")
                print(job.job_type, text_output)
                runtime_tag = "Job Runtime: "

                start_ind = text_output.rfind(runtime_tag)
                if job.job_type == "consumer" or job.job_type == "producer":
                    if start_ind == -1:
                        print("Trial resulted in error")
                        print(text_output)

                        result.append({
                            "trial": i,
                            "time": "ERROR",
                            "type": job.job_type,
                            "benchmark": benchmark,
                        })

                    else:
                        start_ind = start_ind + len(runtime_tag)
                        # Not sure if this is safe to get the time if it isn't always in ms
                        end_ind = start_ind+text_output[start_ind:].find("ms")
                        time_taken = int(text_output[start_ind:end_ind])

                        print(
                            f"{job.job_type} job (Trial {i}) finished in {time_taken} ms")
                        result.append({
                            "trial": i,
                            "time": time_taken,
                            "type": job.job_type,
                            "benchmark": benchmark,
                            "checkpointing_interval": args.checkpointing_interval
                        })

            res_dict = {
                "trial": i,
                "benchmark": benchmark,
            }

            # get metrics from kubernetes logs
            tags = ["[SNAPSHOT_TIME]", "[FLINK_QUESTDB_RUNTIME]", "[DATA_FRESHNESS]"]
            logged_tag_values = k8s.get_tags_from_pod_logs(
                pods,
                start_time,
                tags
            )

            for tag in tags:
                res_dict[tag] = logged_tag_values[tag]

            result.append(res_dict)

        json.dump(result, file, indent=4)
        file.flush()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('master', type=str, default="10.10.1.1", nargs="?")
    parser.add_argument('k', type=int, default=1, nargs='?')  # number of runs
    parser.add_argument('checkpointing_interval', type=str,
                        default="1000", nargs='?')
    parser.add_argument('benchmark', type=str, default='Wiki', nargs='?')

    parser.add_argument('producers', type=int, default=1, nargs='?')
    parser.add_argument('consumers', type=int, default=1, nargs='?')
    parser.add_argument('application', type=str, default="QuestDBClient", nargs='?')

    parser.add_argument('port', type=str, default="8888", nargs='?')
    args = parser.parse_args()

    # make a folder to save the experiments for today
    os.makedirs(today_folder, exist_ok=True)

    if args.benchmark not in flink.BENCHMARK_MAP:
        print("Can't find benchmark with name", args.benchmark)
        return

    run_experiment_trials(args)


if __name__ == "__main__":
    main()
