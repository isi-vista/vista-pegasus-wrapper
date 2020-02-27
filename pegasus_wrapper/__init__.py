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

from attr import attrib, attrs
from attr.validators import in_, instance_of

from immutablecollections import ImmutableSet
from vistautils.memory_amount import MemoryAmount
from vistautils.range import Range

from pegasus_wrapper.version import version as __version__  # noqa

from Pegasus.api import Job
from Pegasus.DAX3 import Namespace, Profile
from typing_extensions import Protocol

from saga_tools import to_slurm_memory_string


class DependencyNode:
    """
    An abstract object tied to a computation
    which can be used only for the purpose
    of indicating that one computation
    depends on the output of another computation.
    """


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
    def apply_to_job(self, job: Job, log_base_directory: Path) -> None:
        """
        Applies the appropriate settings to *job*
        to account for the requested resources.
        """


SLURM_RESOURCE_STRING = """--{qos_or_account} --partition {partition} --ntasks 1
 --cpus-per-task {num_cpus} --gpus-per-task {num_gpus} --job-name {job_name} --mem {mem_str}"""


@attrs(frozen=True, slots=True)
class SlurmResourceRequest(ResourceRequest):
    """
    A `ResourceRequest` for a job running on a SLURM cluster.
    """

    memory: MemoryAmount = attrib(validator=instance_of(MemoryAmount), kw_only=True)
    partition: str = attrib(validator=instance_of(str), kw_only=True)
    num_cpus: int = attrib(validator=in_(Range.at_least(1)), default=1, kw_only=True)
    num_gpus: int = attrib(validator=in_(Range.at_least(0)), default=0, kw_only=True)

    def apply_to_job(  # pylint: disable=arguments-differ
        self, job: Job, log_base_directory: Path, *, job_name: str = ""
    ) -> None:
        qos_or_account = (
            f"qos {self.partition}"
            if self.partition in ("scavenge", "ephemeral")
            else f"account {self.partition}"
        )
        slurm_resource_content = SLURM_RESOURCE_STRING.format(
            qos_or_account=qos_or_account,
            partition=self.partition,
            num_cpus=self.num_cpus,
            num_gpus=self.num_gpus,
            job_name=job_name,
            mem_str=to_slurm_memory_string(self.memory),
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

    def schedule_job(self, job: Job, resource_request: ResourceRequest) -> DependencyNode:
        raise NotImplementedError()


def execute(workflow: WorkflowBuilder):
    # build DAG, call writeXml
    # We will let the user submit themselves, however we could provide them the submit command if known
    raise NotImplementedError()


class Artifact(Protocol):
    """
    An `Artifact` is the result of any computation.
    """

    computed_by: ImmutableSet[DependencyNode]
