import logging
from abc import abstractmethod
from typing import Optional

from attr import attrib, attrs
from attr.validators import in_, instance_of, optional

from vistautils.memory_amount import MemoryAmount
from vistautils.parameters import Parameters
from vistautils.range import Range

from Pegasus.DAX3 import Job, Namespace, Profile
from saga_tools.slurm import to_slurm_memory_string
from typing_extensions import Protocol

SCAVENGE = "scavenge"
EPHEMERAL = "ephemeral"
_SLURM_DEFAULT_MEMORY = MemoryAmount.parse("2G")
_PROJECT_PARTITION_JOB_TIME_IN_MINUTES = 1440


@attrs(frozen=True, slots=True)
class Partition:
    """
    Representation of a SAGA partition
    """

    name: str = attrib(validator=instance_of(str))
    max_walltime: int = attrib(validator=instance_of(int), kw_only=True)

    def __eq__(self, other) -> bool:
        return self.name == other.name

    def __str__(self) -> str:
        return self.name

    @staticmethod
    def from_str(name: str):
        _partition_to_max_walltime = {"ephemeral": 720, "scavenge": 120}

        return Partition(
            name=name,
            max_walltime=_partition_to_max_walltime.get(
                name, _PROJECT_PARTITION_JOB_TIME_IN_MINUTES
            ),
        )


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
    def apply_to_job(self, job: Job, *, job_name: str) -> None:
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


@attrs(frozen=True, slots=True)
class SlurmResourceRequest(ResourceRequest):
    """
    A `ResourceRequest` for a job running on a SLURM cluster.
    """

    partition: Optional[Partition] = attrib(
        converter=lambda x: Partition.from_str(x) if x else None,
        kw_only=True,
        default=None,
    )
    memory: Optional[MemoryAmount] = attrib(
        validator=optional(instance_of(MemoryAmount)), kw_only=True, default=None
    )
    num_cpus: Optional[int] = attrib(
        validator=optional(in_(Range.at_least(1))), default=None, kw_only=True
    )
    num_gpus: Optional[int] = attrib(
        validator=optional(in_(Range.at_least(0))), default=None, kw_only=True
    )
    job_time_in_minutes: Optional[int] = attrib(
        validator=optional(instance_of(int)), default=None, kw_only=True
    )
    exclude_list: Optional[str] = attrib(
        validator=optional(instance_of(str)), kw_only=True, default=None
    )
    run_on_single_node: Optional[str] = attrib(
        validator=optional(instance_of(str)), kw_only=True, default=None
    )

    def __attrs_post_init__(self):
        if not self.job_time_in_minutes:
            partition_job_time = None
            if not self.partition:
                logging.warning(
                    "Could not find selected partition. Setting job with no job time specified to max project partition walltime."
                )
                partition_job_time = _PROJECT_PARTITION_JOB_TIME_IN_MINUTES
            else:
                logging.warning(
                    "Defaulting job with no job time specified to max walltime of selected partition '%s'",
                    self.partition.name,
                )
                partition_job_time = self.partition.max_walltime
            # Workaround suggested by maintainers of attrs.
            # See https://www.attrs.org/en/stable/how-does-it-work.html#how-frozen
            object.__setattr__(self, "job_time_in_minutes", partition_job_time)

    @run_on_single_node.validator
    def check(self, _, value: str):
        if value and len(value.split(",")) != 1:
            raise ValueError("run_on_single_node parameter must provide only node!")

    @staticmethod
    def from_parameters(params: Parameters) -> ResourceRequest:
        return SlurmResourceRequest(
            partition=params.string("partition"),
            num_cpus=params.optional_positive_integer("num_cpus"),
            num_gpus=params.optional_integer("num_gpus"),
            memory=MemoryAmount.parse(params.string("memory"))
            if "memory" in params
            else None,
            job_time_in_minutes=params.optional_integer("job_time_in_minutes"),
            exclude_list=params.optional_string("exclude_list"),
            run_on_single_node=params.optional_string("run_on_single_node"),
        )

    def unify(self, other: ResourceRequest) -> ResourceRequest:
        if isinstance(other, SlurmResourceRequest):
            partition = other.partition or self.partition
        else:
            partition = self.partition

        return SlurmResourceRequest(
            partition=partition.name,
            memory=other.memory or self.memory,
            num_cpus=other.num_cpus or self.num_cpus,
            num_gpus=other.num_gpus if other.num_gpus is not None else self.num_gpus,
            job_time_in_minutes=other.job_time_in_minutes or self.job_time_in_minutes,
            exclude_list=other.exclude_list or self.exclude_list,
            run_on_single_node=other.run_on_single_node or self.run_on_single_node,
        )

    def convert_time_to_slurm_format(self, job_time_in_minutes: int) -> str:
        hours, mins = divmod(job_time_in_minutes, 60)
        return f"{hours}:{str(mins)+'0' if mins < 10 else mins}:00"

    def apply_to_job(self, job: Job, *, job_name: str) -> None:
        if not self.partition:
            raise RuntimeError("A partition to run on must be specified.")

        if self.partition.max_walltime < self.job_time_in_minutes:
            raise ValueError(
                f"Partition '{self.partition.name}' has a max walltime of {self.partition.max_walltime} mins, which is less than the time given ({self.job_time_in_minutes} mins) for job: {job_name}."
            )

        qos_or_account = (
            f"qos {self.partition.name}"
            if self.partition.name in (SCAVENGE, EPHEMERAL)
            else f"account {self.partition.name}"
        )
        slurm_resource_content = SLURM_RESOURCE_STRING.format(
            qos_or_account=qos_or_account,
            partition=self.partition.name,
            num_cpus=self.num_cpus or 1,
            num_gpus=self.num_gpus if self.num_gpus is not None else 0,
            job_name=job_name,
            mem_str=to_slurm_memory_string(self.memory or _SLURM_DEFAULT_MEMORY),
            time=self.convert_time_to_slurm_format(self.job_time_in_minutes),
        )

        if (
            self.exclude_list
            and self.run_on_single_node
            and self.run_on_single_node in self.exclude_list
        ):
            raise ValueError(
                "the 'exclude_list' and 'run_on_single_node' options are not consistent."
            )

        if self.exclude_list:
            slurm_resource_content += f" --exclude={self.exclude_list}"

        if self.run_on_single_node:
            slurm_resource_content += f" --nodelist={self.run_on_single_node}"

        logging.debug(
            "Slurm Resource Request for %s: %s", job_name, slurm_resource_content
        )
        job.addProfile(
            Profile(Namespace.PEGASUS, "glite.arguments", slurm_resource_content)
        )
        category_profile = Profile(Namespace.DAGMAN, "category", self.partition)
        if not job.hasProfile(category_profile):
            job.addProfile(category_profile)


SLURM_RESOURCE_STRING = """--{qos_or_account} --partition {partition} --ntasks 1
 --cpus-per-task {num_cpus} --gpus-per-task {num_gpus} --job-name {job_name} --mem {mem_str}
 --time {time}"""
_BACKEND_PARAM = "backend"
