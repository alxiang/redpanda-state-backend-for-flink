import subprocess

from utils.flink import ROOTPATH, Job


def launch_application_job(args, pod) -> Job:

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
   
    return Job(f"application", proc)
