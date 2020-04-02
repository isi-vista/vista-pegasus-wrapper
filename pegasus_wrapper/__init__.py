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
from typing import Dict, Iterable, List, Optional

from attr import Factory, attrib, attrs
from attr.validators import instance_of

from immutablecollections import ImmutableSet, immutableset
from vistautils.memory_amount import MemoryAmount
from vistautils.parameters import Parameters

from pegasus_wrapper.pegasus_utils import build_submit_script, path_to_pfn
from pegasus_wrapper.version import version as __version__  # noqa

from networkx import DiGraph
from Pegasus.api import File, Job
from Pegasus.DAX3 import ADAG, Executable, Link
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


@attrs(frozen=True, slots=True)
class WorkflowBuilder:
    """
    A class which wraps a representation of a Pegasus workflow

    Run `build(workflow)` to write out the workflow into DAX files for submission
    """

    name: str = attrib(validator=instance_of(str), kw_only=True)
    created_by: str = attrib(validator=instance_of(str), kw_only=True)
    log_base_directory: Path = attrib(validator=instance_of(Path), kw_only=True)
    _workflow_directory: Path = attrib(validator=instance_of(Path), kw_only=True)
    _graph: DiGraph = attrib(
        validator=instance_of(DiGraph), kw_only=True, default=DiGraph()
    )
    _job_to_executable: Dict[Job, Executable] = attrib(
        validator=instance_of(Dict), kw_only=True, default=Factory(Dict), init=False
    )
    _jobs_in_graph: List[Job] = attrib(
        validator=instance_of(List), kw_only=True, default=Factory(List), init=False
    )
    _files_in_graph: List[File] = attrib(
        validator=instance_of(List), kw_only=True, default=Factory(List), init=False
    )

    @staticmethod
    def from_params(params: Parameters) -> "WorkflowBuilder":
        return WorkflowBuilder(
            name=params.string("workflow_name", default="Workflow"),
            created_by=params.string("workflow_created", default="Default Constructor"),
            log_base_directory=params.creatable_directory("workflow_log_dir"),
            workflow_directory=params.creatable_directory("workflow_dir"),
        )

    def pegasus_executable_to_pegasus_job(
        self,
        executable: Executable,
        inputs: Iterable[File] = immutableset(),
        outputs: Iterable[File] = immutableset(),
    ) -> Job:
        rtrnr = Job(executable)
        self._job_to_executable[rtrnr] = executable

        for file in inputs:
            rtrnr.uses(file, link=Link.INPUT)

        for file in outputs:
            rtrnr.uses(file, link=Link.OUTPUT, transfer=True)

        return rtrnr

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
        for file in files:
            if file not in self._graph:
                self._graph.add_node(file)
                self._files_in_graph.append(file)

            if is_input:
                self._graph.add_edge(file, job, label=INPUT_FILE_LABEL)
                for pred in self._graph.predecessors(file):
                    if self._graph.get_edge_data(pred, file)["label"] == OUTPUT_FILE_LABEL:
                        self._graph.add_edge(job, pred, label=DEPENDENT_JOB_LABEL)
            else:
                self._graph.add_edge(job, file, label=OUTPUT_FILE_LABEL)
                for suc in self._graph.successors(file):
                    if self._graph.get_edge_data(file, suc)["label"] == INPUT_FILE_LABEL:
                        self._graph.add_edge(suc, job, label=DEPENDENT_JOB_LABEL)

    def build(self, output_xml_dir: Path) -> None:
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
            diamond.addExecutable(self._job_to_executable[job])
            diamond.addJob(job)
            for successor in self._graph.successors(job):
                if self._graph.get_edge_data(job, successor)["label"] == DEPENDENT_JOB_LABEL:
                    diamond.depends(successor, job)

        dax_file_name = f"{self.name}.dax"
        dax_file = output_xml_dir / dax_file_name
        with dax_file.open("w") as dax:
            diamond.writeXML(dax)
        build_submit_script(
            output_xml_dir / "submit.sh", dax_file_name, self._workflow_directory
        )

    def get_job_inputs(self, job: Job) -> ImmutableSet[File]:
        return immutableset(
            adj_node
            for adj_node in self._graph.predecessors(job)
            if self._graph.get_edge_data(adj_node, job) == INPUT_FILE_LABEL
        )

    def get_job_outputs(self, job: Job) -> ImmutableSet[File]:
        return immutableset(
            adj_node
            for adj_node in self._graph.successors(job)
            if self._graph.get_edge_data(adj_node, job) == OUTPUT_FILE_LABEL
        )

    def get_job_executable(self, job: Job) -> Executable:
        return self._job_to_executable[job]

    def get_jobs(self) -> ImmutableSet[Job]:
        return immutableset(self._jobs_in_graph)

    def get_files(self) -> ImmutableSet[File]:
        return immutableset(self._files_in_graph)

    def get_executables(self) -> ImmutableSet[Executable]:
        return immutableset(self._job_to_executable.values())


INPUT_FILE_LABEL = "input_file"
OUTPUT_FILE_LABEL = "output_file"
CHILD_JOB_LABEL = "child_job"
DEPENDENT_JOB_LABEL = "dependent_job"