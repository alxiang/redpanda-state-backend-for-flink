import subprocess

from utils.flink import ROOTPATH, Job


def launch_application_job(args, pod) -> Job:

    if args.application == "QuestDBClient":

        proc = subprocess.Popen([
            "kubectl",
            "exec",
            pod,
            "--",
            "java",
            "-cp",
            "/opt/flink/redpanda-state-backend-for-flink/flink-statebackend-redpanda/target/flink-statebackend-redpanda-1.13.2-jar-with-dependencies.jar",
            f"applications.{args.application}",
        ], stdout=subprocess.PIPE)

    elif args.application == "VectorSimKafka":

        proc = subprocess.Popen([
            "kubectl",
            "exec",
            pod,
            "--",
            "python3",
            "/opt/flink/redpanda-state-backend-for-flink/src/python/applications/vector_similarity_search/vector_similarity.py",
            str(100_000*args.producers),
            str(64)
        ])

        print(f"Submitted job to pod {pod}")

   
    return Job(f"application", proc)
