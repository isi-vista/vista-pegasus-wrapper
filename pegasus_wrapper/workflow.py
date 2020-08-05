"""
Core workflow management classes for the Vista Pegasus wrapper.

The user should typically not refer to these classes directly
and should instead use the methods in the root of the package.
"""
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Set, Union

from attr import attrib, attrs
from attr.validators import instance_of, optional

from vistautils.class_utils import fully_qualified_name
from vistautils.io_utils import CharSink
from vistautils.parameters import Parameters, YAMLParametersWriter

from Pegasus.DAX3 import ADAG, Executable, File, Job, Link, Namespace, Profile
from pegasus_wrapper import resources
from pegasus_wrapper.artifact import DependencyNode, _canonicalize_depends_on
from pegasus_wrapper.conda_job_script import CondaJobScriptGenerator
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.pegasus_utils import (
    build_submit_script,
    path_to_pegasus_file,
    path_to_pfn,
)
from pegasus_wrapper.resource_request import ResourceRequest
from saga_tools.conda import CondaConfiguration

try:
    import importlib.resources as pkg_resources
except ImportError:
    # Try backported to PY <3.7 'importlib_resources'
    import importlib_resources as pkg_resources


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
    _default_site: str = attrib(validator=instance_of(str), kw_only=True)
    default_resource_request: ResourceRequest = attrib(
        validator=instance_of(ResourceRequest), kw_only=True
    )
    _conda_script_generator: Optional[CondaJobScriptGenerator] = attrib(
        validator=optional(instance_of(CondaJobScriptGenerator)),
        default=None,
        kw_only=True,
    )
    # Pegasus' internal structure of the job requirements
    _job_graph: ADAG = attrib(init=False)
    # Occassionally an identical job may be scheduled multiple times
    # in the workflow graph. We compute this based on a job signature
    # and only actually schedule the job once.
    _signature_to_job: Dict[Any, DependencyNode] = attrib(init=False, factory=dict)
    # Files already added to the job graph
    _added_files: Set[File] = attrib(init=False, factory=set)
    # Path to the replica catalog to store checkpointed files
    _replica_catalog: Path = attrib(validator=instance_of(Path))
    _category_to_max_jobs: Dict[str, int] = attrib(factory=dict)

    @staticmethod
    def from_parameters(params: Parameters) -> "WorkflowBuilder":
        workflow_directory = params.creatable_directory("workflow_directory")

        replica_catalog = workflow_directory / "rc.dat"
        if replica_catalog.exists():
            replica_catalog.unlink()
        replica_catalog.touch(mode=0o744)

        return WorkflowBuilder(
            name=params.string("workflow_name", default="Workflow"),
            created_by=params.string("workflow_created", default="Default Constructor"),
            workflow_directory=workflow_directory,
            default_site=params.string("site"),
            conda_script_generator=CondaJobScriptGenerator.from_parameters(params),
            namespace=params.string("namespace"),
            default_resource_request=ResourceRequest.from_parameters(params),
            replica_catalog=replica_catalog,
        )

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
        parameters: Union[Parameters, Dict[str, Any]],
        *,
        depends_on,
        resource_request: Optional[ResourceRequest] = None,
        override_conda_config: Optional[CondaConfiguration] = None,
        category: Optional[str] = None,
    ) -> DependencyNode:
        """
        Schedule a job to run the given *python_module* on the given *parameters*.

        If this job requires other jobs to be executed first,
        include them in *depends_on*.

        This method returns a `DependencyNode` which can be used in *depends_on*
        for future jobs.
        """
        job_dir = self.directory_for(job_name)
        ckpt_name = job_name / "___ckpt"
        checkpoint_path = job_dir / "___ckpt"

        depends_on = _canonicalize_depends_on(depends_on)
        if isinstance(python_module, str):
            fully_qualified_module_name = python_module
        else:
            fully_qualified_module_name = fully_qualified_name(python_module)

        # allow users to specify the parameters as a dict for convenience
        if not isinstance(parameters, Parameters):
            parameters = Parameters.from_mapping(parameters)

        # If we've already scheduled this identical job,
        # then don't schedule it again.
        params_sink = CharSink.to_string()
        YAMLParametersWriter().write(parameters, params_sink)
        signature = (fully_qualified_module_name, params_sink.last_string_written)
        if signature in self._signature_to_job:
            logging.info("Job %s recognized as a duplicate", job_name)
            return self._signature_to_job[signature]

        script_path = job_dir / "___run.sh"
        stdout_path = parameters.string(
            "logfile", default=str((job_dir / "___stdout.log").absolute())
        )
        self._conda_script_generator.write_shell_script_to(
            entry_point_name=fully_qualified_module_name,
            parameters=parameters,
            working_directory=job_dir,
            script_path=script_path,
            params_path=job_dir / "____params.params",
            stdout_file=stdout_path,
            ckpt_path=checkpoint_path,
            override_conda_config=override_conda_config,
        )
        script_executable = Executable(
            namespace=self._namespace,
            name=str(job_name).replace("/", "_"),
            version="4.0",
            os="linux",
            arch="x86_64",
        )
        script_executable.addPFN(path_to_pfn(script_path, site=self._default_site))
        if not self._job_graph.hasExecutable(script_executable):
            self._job_graph.addExecutable(script_executable)
        job = Job(script_executable)
        self._job_graph.addJob(job)
        for parent_dependency in depends_on:
            if parent_dependency.job:
                self._job_graph.depends(job, parent_dependency.job)
            for out_file in parent_dependency.output_files:
                job.uses(out_file, link=Link.INPUT)

        if resource_request is not None:
            resource_request = self.default_resource_request.unify(resource_request)
        else:
            resource_request = self.default_resource_request

        if category:
            job.profile(Namespace.DAGMAN, "category", category)
        resource_request.apply_to_job(job, job_name=self._job_name_for(job_name))

        # Handle Output Files
        # This is currently only handled as the checkpoint file
        # See: https://github.com/isi-vista/vista-pegasus-wrapper/issues/25
        checkpoint_pegasus_file = path_to_pegasus_file(
            checkpoint_path, site=self._default_site, name=f"{ckpt_name}"
        )

        if checkpoint_pegasus_file not in self._added_files:
            self._job_graph.addFile(checkpoint_pegasus_file)
            self._added_files.add(checkpoint_pegasus_file)

        # If the checkpoint file already exists, we want to add it to the replica catalog
        # so that we don't run the job corresponding to the checkpoint file again
        if checkpoint_path.exists():
            with self._replica_catalog.open("a+") as handle:
                handle.write(
                    f"{ckpt_name} file://{checkpoint_path} site={self._default_site}\n"
                )

        job.uses(checkpoint_pegasus_file, link=Link.OUTPUT, transfer=True)

        dependency_node = DependencyNode.from_job(
            job, output_files=[checkpoint_pegasus_file]
        )
        self._signature_to_job[signature] = dependency_node

        logging.info("Scheduled Python job %s", job_name)
        return dependency_node

    def limit_jobs_for_category(self, category: str, max_jobs: int):
        """
        Limit the number of jobs in the given category that can run concurrently to max_jobs.
        """
        self._category_to_max_jobs[category] = max_jobs

    def _conf_limits(self) -> str:
        """
        Return a Pegasus config string which sets the max jobs per category appropriately.
        """
        return "".join(
            f"dagman.{category}.maxjobs={max_jobs}\n"
            for category, max_jobs in self._category_to_max_jobs.items()
        )

    def write_dax_to_dir(self, output_xml_dir: Optional[Path] = None) -> Path:
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
            data=pkg_resources.read_text(resources, "pegasus.conf")
            + "pegasus.catalog.replica=File\n"
            + f"pegasus.catalog.replica.file={self._replica_catalog}\n"
            + self._conf_limits(),
            encoding="utf-8",
        )

        return dax_file

    def default_conda_configuration(self) -> CondaConfiguration:
        return self._conda_script_generator.conda_config

    @_job_graph.default
    def _init_job_graph(self) -> ADAG:
        ret = ADAG(self.name)
        ret.metadata("name", self.name)
        ret.metadata("createdby", self.created_by)
        return ret
