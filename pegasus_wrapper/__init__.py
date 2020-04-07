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
import logging
from abc import abstractmethod
from itertools import chain
from pathlib import Path, PurePath
from typing import Any, Iterable, Optional, Tuple, TypeVar, Union
from uuid import uuid4

from attr import attrib, attrs
from attr.validators import instance_of, optional

from immutablecollections import ImmutableSet, immutableset
from immutablecollections.converter_utils import _to_immutableset, _to_tuple
from vistautils.class_utils import fully_qualified_name
from vistautils.memory_amount import MemoryAmount
from vistautils.parameters import Parameters

from Pegasus.DAX3 import ADAG, Executable, Job
from pegasus_wrapper.conda_job_script import CondaJobScriptGenerator
from pegasus_wrapper.pegasus_utils import build_submit_script, path_to_pfn
from pegasus_wrapper.version import version as __version__  # noqa

from typing_extensions import Protocol


@attrs(slots=True, eq=False)
class DependencyNode:
    """
    An abstract object tied to a computation
    which can be used only for the purpose
    of indicating that one computation
    depends on the output of another computation.
    """

    job: Job = attrib(validator=instance_of(Job), kw_only=True)

    @staticmethod
    def from_job(job: Job) -> "DependencyNode":
        return DependencyNode(job=job)


def _parse_parts(string_or_sequence: Union[str, Iterable[str]]) -> Tuple[str]:
    if isinstance(string_or_sequence, str):
        return tuple(string_or_sequence.split("/"))
    else:
        return tuple(string_or_sequence)


@attrs(slots=True, frozen=True, repr=False)
class Locator:
    """
    fill me in
    """

    _parts: Tuple[str] = attrib(converter=_parse_parts)

    def __truediv__(self, other: Union[str, "Locator"]):
        if isinstance(other, Locator):
            return Locator(
                chain(self._parts, other._parts)  # pylint:disable=protected-access
            )
        elif isinstance(other, str):
            new_parts = list(self._parts)
            new_parts.append(other)
            return Locator(new_parts)
        else:
            raise RuntimeError(f"Cannot extend a locator with a {type(other)}")

    def __repr__(self) -> str:
        return "/".join(self._parts)


class Artifact(Protocol):
    """
    An `Artifact` is the result of any computation.
    """

    computed_by: ImmutableSet[DependencyNode]
    locator: Optional[Locator]


_T = TypeVar("_T")


@attrs(frozen=True, slots=True)
class ValueArtifact(Artifact):
    value: _T = attrib()
    computed_by: ImmutableSet[DependencyNode] = attrib(
        converter=_to_immutableset, kw_only=True
    )
    locator: Optional[Locator] = attrib(
        validator=optional(instance_of(Locator)), kw_only=True
    )

    @staticmethod
    def preexisting(
        value: _T, *, locator: Optional[Locator] = None
    ) -> "ValueArtifact[_T]":
        return ValueArtifact(value, computed_by=immutableset(), locator=locator)

    @staticmethod
    def computed(
        value: _T,
        *,
        computed_by: Union[DependencyNode, Iterable[DependencyNode]],
        locator: Optional[Locator] = None,
    ) -> "ValueArtifact[_T]":
        if isinstance(computed_by, DependencyNode):
            canonical_computed_by = immutableset([computed_by])
        else:
            canonical_computed_by = immutableset(computed_by)
        return ValueArtifact(value, computed_by=canonical_computed_by, locator=locator)


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
    _namespace: str = attrib(validator=instance_of(str), kw_only=True)
    _conda_script_generator: Optional[CondaJobScriptGenerator] = attrib(
        validator=optional(instance_of(CondaJobScriptGenerator)),
        default=None,
        kw_only=True,
    )
    _default_site: str = attrib(validator=instance_of(str), kw_only=True)

    _job_graph = attrib(init=False)

    # _graph: DiGraph = attrib(
    #     validator=instance_of(DiGraph), kw_only=True, default=DiGraph()
    # )
    # _job_to_executable: Dict[Job, Executable] = attrib(
    #     validator=instance_of(Dict), kw_only=True, default=Factory(dict), init=False
    # )
    # _jobs_in_graph: List[Job] = attrib(
    #     validator=instance_of(List), kw_only=True, default=Factory(list), init=False
    # )
    # _files_in_graph: List[File] = attrib(
    #     validator=instance_of(List), kw_only=True, default=Factory(list), init=False
    # )

    @staticmethod
    def from_params(params: Parameters) -> "WorkflowBuilder":
        return WorkflowBuilder(
            name=params.string("workflow_name", default="Workflow"),
            created_by=params.string("workflow_created", default="Default Constructor"),
            log_base_directory=params.creatable_directory("workflow_log_dir"),
            workflow_directory=params.creatable_directory("workflow_dir"),
            default_site=params.string("site"),
            conda_script_generator=CondaJobScriptGenerator.from_parameters(params),
            namespace=params.string("namespace"),
        )

    # def pegasus_executable_to_pegasus_job(
    #     self,
    #     executable: Executable,
    #     inputs: Iterable[File] = immutableset(),
    #     outputs: Iterable[File] = immutableset(),
    # ) -> Job:
    #     rtrnr = Job(executable)
    #     self._job_to_executable[rtrnr] = executable
    #
    #     for file in inputs:
    #         rtrnr.uses(file, link=Link.INPUT)
    #
    #     for file in outputs:
    #         rtrnr.uses(file, link=Link.OUTPUT, transfer=True)
    #
    #     return rtrnr

    def schedule_job(self, job: Job, resource_request: ResourceRequest) -> DependencyNode:
        """
        Schedule a `Job` for computation during the workflow
        """

        resource_request.apply_to_job(job, log_base_directory=self.log_base_directory)

        self._job_graph.addJob(job)
        self._add_files_to_graph(job, immutableset(job.get_inputs()), is_input=True)
        self._add_files_to_graph(job, immutableset(job.get_outputs()), is_input=False)

        return DependencyNode(job=job)

    def run_python_on_parameters(
        self,
        job_name: Locator,
        python_module: Any,
        parameters: Parameters,
        *,
        depends_on: Iterable[DependencyNode] = immutableset(),
    ) -> DependencyNode:
        if isinstance(python_module, str):
            fully_qualified_module_name = python_module
        else:
            fully_qualified_module_name = fully_qualified_name(python_module)

        job_dir = self._workflow_directory / str(job_name)
        script_path = job_dir / "run.sh"
        self._conda_script_generator.write_shell_script_to(
            entry_point_name=fully_qualified_module_name,
            parameters=parameters,
            working_directory=job_dir,
            script_path=script_path,
            params_path=job_dir / "params.params",
        )
        script_executable = Executable(
            namespace=self._namespace,
            name=str(job_name).replace("/", "_"),
            version="4.0",
            os="linux",
            arch="x86_64",
        )
        script_executable.addPFN(path_to_pfn(script_path, site=self._default_site))
        self._job_graph.addExecutable(script_executable)
        job = Job(script_executable)
        self._job_graph.addJob(job)
        for parent_dependency in depends_on:
            self._job_graph.depends(job, parent_dependency.job)
        return DependencyNode.from_job(job)

    # def _add_files_to_graph(
    #     self, job: Job, files: Iterable[File], *, is_input: bool
    # ) -> None:
    #     for file in files:
    #         if file not in self._graph:
    #             self._graph.add_node(file)
    #             self._files_in_graph.append(file)
    #
    #         if is_input:
    #             self._graph.add_edge(file, job, label=INPUT_FILE_LABEL)
    #             for pred in self._graph.predecessors(file):
    #                 if (
    #                     self._graph.get_edge_data(pred, file)["label"]
    #                     == OUTPUT_FILE_LABEL
    #                 ):
    #                     self._graph.add_edge(job, pred, label=DEPENDENT_JOB_LABEL)
    #         else:
    #             self._graph.add_edge(job, file, label=OUTPUT_FILE_LABEL)
    #             for suc in self._graph.successors(file):
    #                 if self._graph.get_edge_data(file, suc)["label"] == INPUT_FILE_LABEL:
    #                     self._graph.add_edge(suc, job, label=DEPENDENT_JOB_LABEL)

    # def build(self, output_xml_dir: Path) -> None:
    #     """
    #     build DAG, call writeXml
    #     We will let the user submit themselves, however we could provide them the submit command if known
    #     """
    #     diamond = ADAG(self.name)
    #     diamond.metadata("name", self.name)
    #     diamond.metadata("createdby", self.created_by)
    #
    #     for file in self._files_in_graph:
    #         diamond.addFile(file)
    #
    #     for job in self._jobs_in_graph:
    #         diamond.addExecutable(self._job_to_executable[job])
    #         diamond.addJob(job)
    #         for successor in self._graph.successors(job):
    #             if (
    #                 self._graph.get_edge_data(job, successor)["label"]
    #                 == DEPENDENT_JOB_LABEL
    #             ):
    #                 diamond.depends(successor, job)
    #
    #     dax_file_name = f"{self.name}.dax"
    #     dax_file = output_xml_dir / dax_file_name
    #     with dax_file.open("w") as dax:
    #         diamond.writeXML(dax)
    #     build_submit_script(
    #         output_xml_dir / "submit.sh", dax_file_name, self._workflow_directory
    #     )

    def write_dax_to_dir(self, output_xml_dir: Path) -> None:
        dax_file_name = f"{self.name}.dax"
        dax_file = output_xml_dir / dax_file_name
        logging.info("Writing DAX to %s", dax_file)
        with dax_file.open("w") as dax:
            self._job_graph.writeXML(dax)
        build_submit_script(
            output_xml_dir / "submit.sh", dax_file_name, self._workflow_directory
        )

    # def get_job_inputs(self, job: Job) -> ImmutableSet[File]:
    #     return immutableset(
    #         adj_node
    #         for adj_node in self._graph.predecessors(job)
    #         if self._graph.get_edge_data(adj_node, job) == INPUT_FILE_LABEL
    #     )
    #
    # def get_job_outputs(self, job: Job) -> ImmutableSet[File]:
    #     return immutableset(
    #         adj_node
    #         for adj_node in self._graph.successors(job)
    #         if self._graph.get_edge_data(adj_node, job) == OUTPUT_FILE_LABEL
    #     )
    #
    # def get_job_executable(self, job: Job) -> Executable:
    #     return self._job_to_executable[job]
    #
    # def get_jobs(self) -> ImmutableSet[Job]:
    #     return immutableset(self._jobs_in_graph)
    #
    # def get_files(self) -> ImmutableSet[File]:
    #     return immutableset(self._files_in_graph)
    #
    # def get_executables(self) -> ImmutableSet[Executable]:
    #     return immutableset(self._job_to_executable.values())

    @_job_graph.default
    def _init_job_graph(self) -> ADAG:
        ret = ADAG(self.name)
        ret.metadata("name", self.name)
        ret.metadata("createdby", self.created_by)
        return ret


INPUT_FILE_LABEL = "input_file"
OUTPUT_FILE_LABEL = "output_file"
CHILD_JOB_LABEL = "child_job"
DEPENDENT_JOB_LABEL = "dependent_job"
