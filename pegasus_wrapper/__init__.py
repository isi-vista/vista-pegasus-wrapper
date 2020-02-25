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

from attr import attrib, attrs
from attr.validators import in_, instance_of

from immutablecollections import ImmutableSet
from vistautils.memory_amount import MemoryAmount
from vistautils.range import Range

from Pegasus.api import Job
from pegasus_wrapper.version import version as __version__  # noqa

from typing_extensions import Protocol


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
    def apply_to_job(self, job: Job) -> None:
        """
        Applies the appropriate settings to *job*
        to account for the requested resources.
        """


@attrs(frozen=True, slots=True)
class SlurmResourceRequest(ResourceRequest):
    """
    A `ResourceRequest` for a job running on a SLURM cluster.
    """

    memory: MemoryAmount = attrib(validator=instance_of(MemoryAmount), kw_only=True)
    partition: str = attrib(validator=instance_of(str), kw_only=True)
    num_cpus: int = attrib(validator=in_(Range.at_least(1)), default=1, kw_only=True)
    num_gpus: int = attrib(validator=in_(Range.at_least(0)), default=0, kw_only=True)

    def apply_to_job(self, job: Job) -> None:
        raise NotImplementedError()


@attrs(frozen=True, slots=True)
class WorkflowBuilder:
    def schedule_job(self, job: Job, resource_request: ResourceRequest) -> DependencyNode:
        raise NotImplementedError()


def execute(workflow: WorkflowBuilder):
    # build DAG, call writeXml
    # should we execute appropriate commands to submit or prefer the user to do it themselves?
    raise NotImplementedError()


class Artifact(Protocol):
    """
    An `Artifact` is the result of any computation.
    """

    computed_by: ImmutableSet[DependencyNode]
