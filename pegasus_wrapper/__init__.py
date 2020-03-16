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
from typing import Iterable, Optional, List

from attr import attrib, attrs, Factory
from attr.validators import instance_of

from immutablecollections import ImmutableSet, immutableset
from vistautils.memory_amount import MemoryAmount

from pegasus_wrapper.version import version as __version__  # noqa

from networkx import DiGraph
from Pegasus.api import OS, Arch, File, Job
from Pegasus.DAX3 import ADAG, PFN, Executable, Link
from typing_extensions import Protocol


@attrs(slots=True)
class DependencyNode:
    """
    An abstract object tied to a computation
    which can be used only for the purpose
    of indicating that one computation
    depends on the output of another computation.
    """

    job: Job = attrib(validator=instance_of(Job), kw_only=True)


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
    def apply_to_job(
        self, job: Job, log_base_directory: Path, job_name: Optional[str] = ""
    ) -> None:
        """
        Applies the appropriate settings to *job*
        to account for the requested resources.
        """


def script_to_pegasus_executable(
    path: Path,
    name: Optional[str] = None,
    *,
    site: str = "local",
    namespace: Optional[str] = None,
    version: Optional[str] = None,
    arch: Optional[Arch] = None,
    os: Optional[OS] = None,
    osrelease: Optional[str] = None,
    osversion: Optional[str] = None,
    glibc: Optional[str] = None,
    installed: Optional[bool] = None,
    container: Optional[str] = None
) -> Executable:
    """
    Turns a script path into a pegasus Executable

    Arguments:
        *name*: Logical name of executable
        *namespace*: Executable namespace
        *version*: Executable version
        *arch*: Architecture that this exe was compiled for
        *os*: Name of os that this exe was compiled for
        *osrelease*: Release of os that this exe was compiled for
        *osversion*: Version of os that this exe was compiled for
        *glibc*: Version of glibc this exe was compiled against
        *installed*: Is the executable installed (true), or stageable (false)
        *container*: Optional attribute to specify the container to use
    """

    rtrnr = Executable(
        path.stem + path.suffix if name is None else name,
        namespace=namespace,
        version=version,
        arch=arch,
        os=os,
        osrelease=osrelease,
        osversion=osversion,
        glibc=glibc,
        installed=installed,
        container=container,
    )
    rtrnr.addPFN(_path_to_pfn(path, site=site))
    return rtrnr


def pegasus_executable_to_pegasus_job(
    executable: Executable,
    inputs: Iterable[File] = immutableset(),
    outputs: Iterable[File] = immutableset(),
) -> Job:
    rtrnr = Job(executable)

    for file in inputs:
        rtrnr.uses(file, link=Link.INPUT)

    for file in outputs:
        rtrnr.uses(file, link=Link.OUTPUT, transfer=True)

    return rtrnr


def path_to_pegaus_file(
    path: Path, *, site: str = "local", name: Optional[str] = None
) -> File:
    """
    Given a *path* object return a pegasus `File` for usage in a workflow

    If the resource is not on a local machine provide the *site* string.

    Files can be used for either an input or output of a Job.
    """
    rtnr = File(path.stem + path.suffix if name is None else name)
    rtnr.addPFN(_path_to_pfn(path, site))
    return rtnr


def _path_to_pfn(path: Path, site: str = "local") -> PFN:
    return PFN(path.absolute(), site=site)


@attrs(frozen=True, slots=True)
class WorkflowBuilder:
    """
    A class which wraps a representation of a Pegasus workflow

    Run `execute(workflow)` to write out the workflow into DAX files for submission
    """

    name: str = attrib(validator=instance_of(str), kw_only=True)
    created_by: str = attrib(validator=instance_of(str), kw_only=True)
    log_base_directory: Path = attrib(validator=instance_of(Path), kw_only=True)
    _graph: DiGraph = attrib(
        validator=instance_of(DiGraph), kw_only=True, default=DiGraph()
    )
    _jobs_in_graph: List[Job] = attrib(validator=instance_of(List), kw_only=True, default=Factory(List), init=False)
    _files_in_graph: List[File] = attrib(validator=instance_of(List), kw_only=True, default=Factory(List), init=False)

    def schedule_job(self, job: Job, resource_request: ResourceRequest) -> DependencyNode:
        """
        Schedule a `Job` for computation during the workflow
        """

        resource_request.apply_to_job(job, log_base_directory=self.log_base_directory)
        self._graph.add_node(job)
        self._jobs_in_graph.append(job)
        self._add_files_to_graph(job, immutableset(job.get_inputs()), is_input=True)
        self._add_files_to_graph(job, immutableset(job.get_outputs()), is_input=False)

        return DependencyNode(job=job)

    def _add_files_to_graph(
        self, job: Job, files: Iterable[File], *, is_input: bool
    ) -> None:
        """
        Add files to the internal digraph to be able to calculate dependent jobs
        """
        for file in files:
            if file not in self._graph:
                self._graph.add_node(file)
                self._files_in_graph.append(file)

            if is_input:
                self._graph.add_edge(file, job, label=INPUT_FILE_LABEL)
            else:
                self._graph.add_edge(job, file, label=OUTPUT_FILE_LABEL)

    def get_jobs(self) -> ImmutableSet[Job]:
        return immutableset(self._jobs_in_graph)

    def get_files(self) -> ImmutableSet[File]:
        return immutableset(self._files_in_graph)

    def calculate_child_jobs(self, root: Job) -> None:
        # Is asking the user to know their initial job a problem?
        raise NotImplementedError()

    def build(self, output_xml_path: Path):
        """
        build DAG, call writeXml
        We will let the user submit themselves, however we could provide them the submit command if known
        """
        diamond = ADAG(self.name)
        diamond.metadata("name", self.name)
        diamond.metadata("createdby", self.created_by)

        for file in self._files_in_graph:
            diamond.addFile(file)

        for job in self._jobs_in_graph:
            diamond.addJob(job)

        # Do Files, Executables, Etc need to be added to a DAX or if we
        # just add the job do those get automatically added?
        # If we do need to add these the functions above which create
        # Executables, files, etc will need to become part of this class so they

        diamond.writeXML(output_xml_path)

        raise NotImplementedError()


INPUT_FILE_LABEL = "input_file"
OUTPUT_FILE_LABEL = "output_file"
CHILD_JOB_LABEL = "child"
