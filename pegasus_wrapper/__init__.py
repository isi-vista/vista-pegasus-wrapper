r"""
This modules provides a more functional wrapper around the Pegasus Python API.

Terminology
===========
- a *computation* is any sort of computation we wish to perform
    which is atomic from the point of view of the workflow engine.
    The canonical example of this is running a program or script on a cluster.
- a *workflow* is a composition of computations.
- a *value* is anything which is an input or output of a computation.
  For Pegasus, the canonical example of a value is a Pegasus `File`.
- a `DependencyNode` is an abstract object tied to a *computation*
    which can be used only for the purpose of indicating
    that one computation depends on the output of another computation.
- an `Artifact` is a pairing of a value together with one or more `DependencyNode`\ s.
"""
# pylint:disable=missing-docstring
from abc import abstractmethod
from pathlib import Path
from typing import Dict, Iterable, Optional

from attr import attrib, attrs, Factory
from attr.validators import in_, instance_of

from immutablecollections import ImmutableSet, immutableset
from vistautils.memory_amount import MemoryAmount
from vistautils.range import Range

from pegasus_wrapper.version import version as __version__  # noqa

from Pegasus.api import Job, File
from Pegasus.DAX3 import Namespace, Profile
from typing_extensions import Protocol

from saga_tools.slurm import to_slurm_memory_string


class DependencyNode:
    """
    An abstract object tied to a computation
    which can be used only for the purpose
    of indicating that one computation
    depends on the output of another computation.
    """


class Artifact(Protocol):
    """
    An `Artifact` is the result of any computation.
    """
    computed_by: ImmutableSet[DependencyNode]


class ResourceRequest(Protocol):
    """
    A specification of the resources needed to execute a computation.

    Particular resource requests are implemented by cluster-specific sub-classes
    of `ResourceRequest`.
    """

    num_cpus: int
    num_gpus: int
    memory: MemoryAmount
    partition: str

    @abstractmethod
    def apply_to_job(self, job: Job, log_base_directory: Path, job_name: Optional[str] = "") -> None:
        """
        Applies the appropriate settings to *job*
        to account for the requested resources.
        """


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
        job_name += ".log"
        slurm_resource_content = SLURM_RESOURCE_STRING.format(
            qos_or_account=qos_or_account,
            partition=self.partition,
            num_cpus=self.num_cpus,
            num_gpus=self.num_gpus,
            job_name=job_name,
            mem_str=to_slurm_memory_string(self.memory),
            stdout_log_path=log_base_directory / job_name
        )
        job.add_profiles(
            Profile(Namespace.PEGASUS, "glite.arguments", slurm_resource_content)
        )


@attrs(frozen=True, slots=True)
class WorkflowBuilder:
    """
    A class which constructs a representation of a Pegasus workflow use `execute(workflow)` to build the workflow
    """
    name: str = attrib(validator=instance_of(str), kw_only=True)
    created_by: str = attrib(validator=instance_of(str), kw_only=True)
    log_base_directory: Path = attrib(validator=instance_of(Path), kw_only=True)
    files_to_created_by_jobs: Dict[File, ImmutableSet[Job]] = attrib(kw_only=True, init=False, default=Factory(dict))

    def schedule_job(self, job: Job, resource_request: ResourceRequest, *, job_name: Optional[str] = None) -> DependencyNode:
        """
        The provided Pegasus Job is expected to already have it's input files and output files linked. We may want to
        wrap building a pegasus job somehow?

        Ryan can you explain why you wanted to return a dependency node here? I'm not sure how that's helpful...
        """
        resource_request.apply_to_job(job, self.log_base_directory, job_name=job_name if job_name is not None else None)

        parent_jobs = self._calculate_dependencies(job.get_inputs())

        for output_file in job.get_outputs():
            self.files_to_created_by_jobs[output_file] = parent_jobs

    def _calculate_dependencies(self, files: Iterable[File]) -> ImmutableSet[Job]:
        return immutableset(self.files_to_created_by_jobs[file] if file in self.files_to_created_by_jobs else None for file in files)

    def build(self, output_xml_path: Path):
        """
        build DAG, call writeXml
        We will let the user submit themselves, however we could provide them the submit command if known

        Currently I need to know the leaf jobs to work my way up the `files_to_created_by_jobs` dict however
        I could also just build my own digraph dependency tree to convert into the DAX

        Or we could just include a DAX in the internal state of the WorkflowBuilder which is added to as `schedule_job`
        is called. Then this file just writes it out to a given location and maybe does sanity checking?
        """
        raise NotImplementedError()
