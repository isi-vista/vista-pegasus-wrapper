import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from attr import Factory, attrib, attrs
from attr.validators import instance_of, optional

from immutablecollections import immutableset
from vistautils.class_utils import fully_qualified_name
from vistautils.io_utils import CharSink
from vistautils.parameters import Parameters, YAMLParametersWriter

from pegasus_wrapper import resources
from pegasus_wrapper.conda_job_script import CondaJobScriptGenerator
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.pegasus_utils import build_submit_script, path_to_pfn
from pegasus_wrapper.resource_request import ResourceRequest

from Pegasus.DAX3 import ADAG, Executable, Job

try:
    import importlib.resources as pkg_resources
except ImportError:
    # Try backported to PY <3.7 'importlib_resources'
    import importlib_resources as pkg_resources


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


@attrs(frozen=True, slots=True)
class WorkflowBuilder:
    """
    A convenient way to build a Pegasus workflow.

    Add jobs using *run_python_on_parameters*.
    When you are done, call *write_dax_to_dir* to write the workflow DAX file,
    sites file, and *slurm.conf* files to the given directory.

    You can then execute them with *pegasus-plan* and *pegasus-run*.
    """

    name: str = attrib(validator=instance_of(str), kw_only=True)
    created_by: str = attrib(validator=instance_of(str), kw_only=True)
    _workflow_directory: Path = attrib(validator=instance_of(Path), kw_only=True)
    _namespace: str = attrib(validator=instance_of(str), kw_only=True)
    _conda_script_generator: Optional[CondaJobScriptGenerator] = attrib(
        validator=optional(instance_of(CondaJobScriptGenerator)),
        default=None,
        kw_only=True,
    )
    _default_site: str = attrib(validator=instance_of(str), kw_only=True)
    default_resource_request: ResourceRequest = attrib(
        validator=instance_of(ResourceRequest), kw_only=True
    )

    _job_graph: ADAG = attrib(init=False)

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
            workflow_directory=params.creatable_directory("workflow_directory"),
            default_site=params.string("site"),
            conda_script_generator=CondaJobScriptGenerator.from_parameters(params),
            namespace=params.string("namespace"),
            default_resource_request=ResourceRequest.from_parameters(params),
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

    # def schedule_job(self, job: Job, resource_request: ResourceRequest) -> DependencyNode:
    #     """
    #     Schedule a `Job` for computation during the workflow
    #     """
    #
    #     resource_request.apply_to_job(job, log_base_directory=self.log_base_directory)
    #
    #     self._job_graph.addJob(job)
    #     self._add_files_to_graph(job, immutableset(job.get_inputs()), is_input=True)
    #     self._add_files_to_graph(job, immutableset(job.get_outputs()), is_input=False)
    #
    #     return DependencyNode(job=job)

    def directory_for(self, locator: Locator) -> Path:
        """
        Get the suggested working/output directory
        for a job with the given `Locator`.
        """
        ret = self._workflow_directory / str(locator)
        ret.mkdir(parents=True, exist_ok=True)
        return ret

    def _job_name_for(self, locator: Locator) -> str:
        return str(locator).replace("/", "_")

    def run_python_on_parameters(
        self,
        job_name: Locator,
        python_module: Any,
        parameters: Parameters,
        *,
        depends_on: Iterable[DependencyNode] = immutableset(),
        resource_request: Optional[ResourceRequest] = None,
    ) -> DependencyNode:
        """
        Schedule a job to run the given *python_module* on the given *parameters*.

        If this job requires other jobs to be executed first,
        include them in *depends_on*.

        This method returns a `DependencyNode` which can be used in *depends_on*
        for future jobs.
        """
        if isinstance(python_module, str):
            fully_qualified_module_name = python_module
        else:
            fully_qualified_module_name = fully_qualified_name(python_module)

        job_dir = self.directory_for(job_name)
        script_path = job_dir / "___run.sh"
        self._conda_script_generator.write_shell_script_to(
            entry_point_name=fully_qualified_module_name,
            parameters=parameters,
            working_directory=job_dir,
            script_path=script_path,
            params_path=job_dir / "____params.params",
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

        if resource_request is not None:
            resource_request = self.default_resource_request.unify(resource_request)
        else:
            resource_request = self.default_resource_request

        resource_request.apply_to_job(
            job, job_name=self._job_name_for(job_name), log_file=job_dir / "___stdout.log"
        )

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
    #     We will let the user submit themselves,
    #     however we could provide them the submit command if known
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

    def write_dax_to_dir(self, output_xml_dir: Optional[Path] = None) -> None:
        if not output_xml_dir:
            output_xml_dir = self._workflow_directory

        dax_file_name = f"{self.name}.dax"
        dax_file = output_xml_dir / dax_file_name
        logging.info("Writing DAX to %s", dax_file)
        with dax_file.open("w") as dax:
            self._job_graph.writeXML(dax)
        build_submit_script(
            output_xml_dir / "submit.sh", dax_file_name, self._workflow_directory
        )

        # We also need to write sites.xml and pegasus.conf
        sites_xml_path = output_xml_dir / "sites.xml"
        sites_xml_path.write_text(
            pkg_resources.read_text(resources, "sites.xml"), encoding="utf-8"
        )

        pegasus_conf_path = output_xml_dir / "pegasus.conf"
        pegasus_conf_path.write_text(
            pkg_resources.read_text(resources, "pegasus.conf"), encoding="utf-8"
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
