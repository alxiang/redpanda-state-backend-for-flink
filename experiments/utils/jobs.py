import subprocess
import time
from typing import List, NamedTuple

class Job(NamedTuple):
    job_type: str
    proc: subprocess.CompletedProcess

def wait_for_jobs(jobs: List[Job]) -> None:
    for job in jobs:
        while job.proc.poll() is None:
            # block until the job is ready for collection
            time.sleep(1)