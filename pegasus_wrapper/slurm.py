from pathlib import Path

from attr import attrib, attrs
from attr.validators import in_, instance_of

from vistautils.memory_amount import MemoryAmount
from vistautils.range import Range

from Pegasus.api import Job
from Pegasus.DAX3 import Namespace, Profile
from pegasus_wrapper import ResourceRequest
from saga_tools.slurm import to_slurm_memory_string

SLURM_RESOURCE_STRING = """--{qos_or_account} --partition {partition} --ntasks 1
 --cpus-per-task {num_cpus} --gpus-per-task {num_gpus} --job-name {job_name} --mem {mem_str}
 --output={stdout_log_path}"""


@attrs(frozen=True, slots=True)
class SlurmResourceRequest(ResourceRequest):
    """
    A `ResourceRequest` for a job running on a SLURM cluster.
    """

    memory: MemoryAmount = attrib(validator=instance_of(MemoryAmount), kw_only=True)
    partition: str = attrib(validator=instance_of(str), kw_only=True)
    num_cpus: int = attrib(validator=in_(Range.at_least(1)), default=1, kw_only=True)
    num_gpus: int = attrib(validator=in_(Range.at_least(0)), default=0, kw_only=True)

    def apply_to_job(
        self, job: Job, log_base_directory: Path, job_name: str = "SlurmResourceJob"
    ) -> None:
        qos_or_account = (
            f"qos {self.partition}"
            if self.partition in ("scavenge", "ephemeral")
            else f"account {self.partition}"
        )
        log_name = f"{job_name}.log"
        slurm_resource_content = SLURM_RESOURCE_STRING.format(
            qos_or_account=qos_or_account,
            partition=self.partition,
            num_cpus=self.num_cpus,
            num_gpus=self.num_gpus,
            job_name=job_name,
            mem_str=to_slurm_memory_string(self.memory),
            stdout_log_path=log_base_directory / log_name,
        )
        job.add_profiles(
            Profile(Namespace.PEGASUS, "glite.arguments", slurm_resource_content)
        )
