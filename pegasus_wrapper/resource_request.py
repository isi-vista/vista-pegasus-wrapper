import logging
from abc import abstractmethod
from pathlib import Path
from typing import Optional

from attr import attrib, attrs
from attr.validators import in_, instance_of, optional

from vistautils.memory_amount import MemoryAmount
from vistautils.parameters import Parameters
from vistautils.range import Range

from Pegasus.DAX3 import Job, Namespace, Profile
from saga_tools.slurm import to_slurm_memory_string

from typing_extensions import Protocol


class ResourceRequest(Protocol):
    """
    A specification of the resources needed to execute a computation.

    Particular resource requests are implemented by cluster-specific sub-classes
    of `ResourceRequest`.

    A ResourceRequest is applied to a job based on the target runtime location. For ISI-internal
    a `SlurmResourceRequest` is provided. However one can use this as a base class to implement
    an AWS or other cluster resource request system.
    """

    num_cpus: int
    num_gpus: int
    memory: MemoryAmount
    partition: str

    @abstractmethod
    def apply_to_job(self, job: Job, *, log_file: Path, job_name: str) -> None:
        """
        Applies the appropriate settings to *job*
        to account for the requested resources.
        """

    def unify(self, other: "ResourceRequest") -> "ResourceRequest":
        """
        Combine this request with *other*.
        Where both specify a resource, the request from *other* is used.
        """

    @staticmethod
    def from_parameters(params: Parameters) -> "ResourceRequest":
        """
        Create a ResourceRequest from a given parameter file

        Current valid backend param values: "slurm"
        """
        backend = params.string(_BACKEND_PARAM, valid_options=["slurm"], default="slurm")
        if backend == "slurm":
            return SlurmResourceRequest.from_parameters(params)
        else:
            raise RuntimeError(f"Invalid backend option {backend}")


_SLURM_DEFAULT_MEMORY = MemoryAmount.parse("2G")


@attrs(frozen=True, slots=True)
class SlurmResourceRequest(ResourceRequest):
    """
    A `ResourceRequest` for a job running on a SLURM cluster.
    """

    memory: Optional[MemoryAmount] = attrib(
        validator=optional(instance_of(MemoryAmount)), kw_only=True, default=None
    )
    partition: Optional[str] = attrib(
        validator=optional(instance_of(str)), kw_only=True, default=None
    )
    num_cpus: Optional[int] = attrib(
        validator=optional(in_(Range.at_least(1))), default=None, kw_only=True
    )
    num_gpus: Optional[int] = attrib(
        validator=optional(in_(Range.at_least(0))), default=None, kw_only=True
    )
    job_time: Optional[str] = attrib(
        validator=optional(instance_of(str)), default="1:00:00", kw_only=True
    )

    @staticmethod
    def from_parameters(params: Parameters) -> ResourceRequest:
        return SlurmResourceRequest(
            partition=params.string("partition"),
            num_cpus=params.optional_positive_integer("num_cpus"),
            num_gpus=params.optional_integer("num_gpus"),
            memory=MemoryAmount.parse(params.string("memory"))
            if "memory" in params
            else None,
            job_time=params.optional_string("job_time"),
        )

    def unify(self, other: ResourceRequest) -> ResourceRequest:
        if isinstance(other, SlurmResourceRequest):
            partition = other.partition if other.partition else self.partition
        else:
            partition = self.partition

        return SlurmResourceRequest(
            partition=partition,
            memory=other.memory if other.memory else self.memory,
            num_cpus=other.num_cpus if other.num_cpus else self.num_cpus,
            num_gpus=other.num_gpus if other.num_gpus is not None else self.num_gpus,
        )

    def apply_to_job(self, job: Job, *, log_file: Path, job_name: str) -> None:
        if not self.partition:
            raise RuntimeError("A partition to run on must be specified.")

        qos_or_account = (
            f"qos {self.partition}"
            if self.partition in ("scavenge", "ephemeral")
            else f"account {self.partition}"
        )
        slurm_resource_content = SLURM_RESOURCE_STRING.format(
            qos_or_account=qos_or_account,
            partition=self.partition,
            num_cpus=self.num_cpus if self.num_cpus else 1,
            num_gpus=self.num_gpus if self.num_gpus is not None else 0,
            job_name=job_name,
            mem_str=to_slurm_memory_string(
                self.memory if self.memory else _SLURM_DEFAULT_MEMORY
            ),
            stdout_log_path=log_file,
            time=self.job_time,
        )
        logging.info(
            "Slurm Resource Request for %s: %s", job_name, slurm_resource_content
        )
        job.addProfile(
            Profile(Namespace.PEGASUS, "glite.arguments", slurm_resource_content)
        )


SLURM_RESOURCE_STRING = """--{qos_or_account} --partition {partition} --ntasks 1
 --cpus-per-task {num_cpus} --gpus-per-task {num_gpus} --job-name {job_name} --mem {mem_str}
 --output={stdout_log_path} --time {time}"""
_BACKEND_PARAM = "backend"
