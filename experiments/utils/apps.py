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

    elif args.application == "VectorSim":

        proc = subprocess.Popen([
            "kubectl",
            "exec",
            pod,
            "--",
            "python",
            "/opt/flink/redpanda-state-backend-for-flink/src/python/applications/vector_similarity_search/vector_similarity.py",
            1_000_000*args.producers,
            64
        ], stdout=subprocess.PIPE)

   
    return Job(f"application", proc)
