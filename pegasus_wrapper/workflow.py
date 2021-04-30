"""
Core workflow management classes for the Vista Pegasus wrapper.

The user should typically not refer to these classes directly
and should instead use the methods in the root of the package.
"""
import logging
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set, Union

from attr import attrib, attrs
from attr.validators import instance_of, optional

from immutablecollections import immutabledict
from vistautils.class_utils import fully_qualified_name
from vistautils.io_utils import CharSink
from vistautils.parameters import Parameters, YAMLParametersWriter

from pegasus_wrapper.artifact import DependencyNode, _canonicalize_depends_on
from pegasus_wrapper.conda_job_script import CondaJobScriptGenerator
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.pegasus_utils import (
    add_local_nas_to_sites,
    add_saga_cluster_to_sites,
    build_submit_script,
    configure_saga_properities,
)
from pegasus_wrapper.resource_request import ResourceRequest
from pegasus_wrapper.scripts import nuke_checkpoints

from Pegasus.api import (
    OS,
    Arch,
    Container,
    File,
    Job,
    Properties,
    ReplicaCatalog,
    SiteCatalog,
    Transformation,
    TransformationCatalog,
    Workflow,
)
from saga_tools.conda import CondaConfiguration

_STR_TO_CONTAINER_TYPE = immutabledict(
    {
        "docker": Container.DOCKER,
        "singularity": Container.SINGULARITY,
        "shifter": Container.SHIFTER,
    }
)


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
    _job_graph: Workflow = attrib(init=False)
    # Occassionally an identical job may be scheduled multiple times
    # in the workflow graph. We compute this based on a job signature
    # and only actually schedule the job once.
    _signature_to_job: Dict[Any, DependencyNode] = attrib(init=False, factory=dict)
    # Files already added to the job graph
    _added_files: Set[File] = attrib(init=False, factory=set)
    # Replica Catalog created via API
    # Files are added here now not the job graph
    _replica_catalog: ReplicaCatalog = attrib(init=False, factory=ReplicaCatalog)
    # Transformation Catalog created via API
    # Executables (v4.9.3) are now called Transformations and stored here rather than the DAX
    _transformation_catalog: TransformationCatalog = attrib(
        init=False, factory=TransformationCatalog
    )
    # Sites Catalog created via API
    # Used to track where operations can take place
    _sites_catalog: SiteCatalog = attrib(init=False, factory=SiteCatalog)
    # Pegasus Properties via API
    # Tracks global properties for all workflows
    _properties: Properties = attrib(init=False, factory=Properties)
    _category_to_max_jobs: Dict[str, int] = attrib(factory=dict)
    # Include an experiment_name so that jobs are more identifiable on SAGA,
    # opting for experiment_name over workflow_name bc of VISTA's use of
    # the same or similar workflow with multiple experiments
    _experiment_name: str = attrib(kw_only=True, default="")
    # Track created files so that if we go to make a duplicate lfn
    # We instead just return the one we already made
    _lfn_to_file: Dict[str, File] = attrib(kw_only=True, factory=dict)
    # Track created transformation so that if we go to make a duplicate
    # we instead return the one we already made
    _transformation_name_to_transformation: Dict[str, Transformation] = attrib(
        kw_only=True, factory=dict
    )

    @staticmethod
    def from_parameters(params: Parameters) -> "WorkflowBuilder":
        wb = WorkflowBuilder(
            name=params.string("workflow_name", default="Workflow"),
            created_by=params.string("workflow_created", default="Default Constructor"),
            workflow_directory=params.creatable_directory("workflow_directory"),
            default_site=params.string("site"),
            conda_script_generator=CondaJobScriptGenerator.from_parameters(params),
            namespace=params.string("namespace"),
            default_resource_request=ResourceRequest.from_parameters(params),
            experiment_name=params.string("experiment_name", default=""),
        )

        if params.boolean("include_nas", default=True):
            add_local_nas_to_sites(
                wb._sites_catalog, params  # pylint: disable=protected-access
            )
        if params.boolean("include_saga", default=True):
            add_saga_cluster_to_sites(
                wb._sites_catalog, params  # pylint: disable=protected-access
            )
            configure_saga_properities(
                wb._properties, params  # pylint: disable=protected-access
            )

        return wb

    def directory_for(self, locator: Locator) -> Path:
        """
        Get the suggested working/output directory
        for a job with the given `Locator`.
        """
        ret = self._workflow_directory / str(locator)
        ret.mkdir(parents=True, exist_ok=True)
        return ret

    def _job_name_for(self, locator: Locator) -> str:
        locater_as_name = str(locator).replace("/", "_")
        return (
            f"{self._experiment_name}_{locater_as_name}"
            if self._experiment_name
            else locater_as_name
        )

    def create_file(
        self,
        logical_file_name: str,
        physical_file_path: Union[Path, str],
        site: Optional[str] = None,
        *,
        add_to_catalog: bool = True,
    ) -> File:
        """
        Create an get Pegasus File type object for a given logical file name to a physical path.
        If the file already exists, return the file otherwise create a new one.
        To just retrieve a previously created file see `get_file`
        """
        if logical_file_name not in self._lfn_to_file:
            f = File(logical_file_name)
            f.add_metadata(creator=self.created_by)
            if add_to_catalog:
                self._replica_catalog.add_replica(
                    site if site else self._default_site,
                    logical_file_name,
                    str(physical_file_path),
                )
            self._lfn_to_file[logical_file_name] = f
        return self._lfn_to_file[logical_file_name]

    def get_file(self, logical_file_name: str) -> File:
        """
        Get a Pegasus File object for a given logical file,
        if it doesn't already exist raise an error.
        """
        if logical_file_name not in self._lfn_to_file:
            raise RuntimeError(
                f"Asked to retrive file name {logical_file_name} but "
                f"this file did not already exist."
            )
        return self._lfn_to_file[logical_file_name]

    def _run_python_job(
        self,
        job_name: Locator,
        python_module_or_path: Any,
        args_or_params: Union[Parameters, Dict[str, Any], str],
        *,
        depends_on,
        resource_request: Optional[ResourceRequest] = None,
        override_conda_config: Optional[CondaConfiguration] = None,
        category: Optional[str] = None,
        use_pypy: bool = False,
        container: Optional[Container] = None,
        pre_job_bash: str = "",
        post_job_bash: str = "",
        job_is_stageable: bool = False,
        job_bypass_staging: bool = False,
        times_to_retry_job: int = 0,
        treat_params_as_cmd_args: bool = False,
    ) -> DependencyNode:
        """
        Internal function to schedule a python job for centralized logic.
        """
        job_dir = self.directory_for(job_name)
        ckpt_name = job_name / "___ckpt"
        checkpoint_path = job_dir / "___ckpt"
        signature_args = None
        depends_on = _canonicalize_depends_on(depends_on)

        if isinstance(python_module_or_path, (str, Path)):
            computed_module_or_path = python_module_or_path
        else:
            computed_module_or_path = fully_qualified_name(python_module_or_path)

        if not isinstance(args_or_params, str):
            # allow users to specify the parameters as a dict for convenience
            if not isinstance(args_or_params, Parameters):
                args_or_params = Parameters.from_mapping(args_or_params)

            params_sink = CharSink.to_string()
            YAMLParametersWriter().write(args_or_params, params_sink)
            signature_args = params_sink.last_string_written

        signature = (
            computed_module_or_path,
            signature_args if signature_args else args_or_params,
        )
        if signature in self._signature_to_job:
            logging.info("Job %s recognized as a duplicate", job_name)
            return self._signature_to_job[signature]

        script_path = job_dir / "___run.sh"
        stdout_path = job_dir / "___stdout.log"

        self._conda_script_generator.write_shell_script_to(
            entry_point_name=computed_module_or_path,
            parameters=args_or_params,
            working_directory=job_dir,
            script_path=script_path,
            params_path=job_dir / "____params.params",
            stdout_file=stdout_path,
            ckpt_path=checkpoint_path,
            override_conda_config=override_conda_config,
            python="pypy3" if use_pypy else "python",
            pre_job=pre_job_bash,
            post_job=post_job_bash,
            treat_params_as_cmd_args=treat_params_as_cmd_args,
        )

        script_executable = Transformation(
            self._job_name_for(job_name),
            namespace=self._namespace,
            version="4.0",
            site=self._default_site,
            pfn=script_path,
            is_stageable=job_is_stageable,
            bypass_staging=job_bypass_staging,
            arch=Arch.X86_64,
            os_type=OS.LINUX,
            container=container,
        )

        self._transformation_catalog.add_transformations(script_executable)

        job = Job(script_executable)
        self._job_graph.add_jobs(job)
        for parent_dependency in depends_on:
            if parent_dependency.job:
                self._job_graph.add_dependency(job, parents=[parent_dependency.job])
            for out_file in parent_dependency.output_files:
                job.add_inputs(out_file)

        resource_request = self.set_resource_request(resource_request)

        job.add_dagman_profile(category=category, retry=str(times_to_retry_job))

        resource_request.apply_to_job(job, job_name=self._job_name_for(job_name))

        # Handle Output Files
        # This is currently only handled as the checkpoint file
        # See: https://github.com/isi-vista/vista-pegasus-wrapper/issues/25
        # If the checkpoint file already exists, we want to add it to the replica catalog
        # so that we don't run the job corresponding to the checkpoint file again
        checkpoint_pegasus_file = self.create_file(
            f"{ckpt_name}", checkpoint_path, add_to_catalog=checkpoint_path.exists()
        )

        job.add_outputs(checkpoint_pegasus_file, stage_out=False)

        dependency_node = DependencyNode.from_job(
            job, output_files=[checkpoint_pegasus_file]
        )
        self._signature_to_job[signature] = dependency_node

        logging.info("Scheduled Python job %s", job_name)
        return dependency_node

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
        use_pypy: bool = False,
        container: Optional[Container] = None,
        pre_job_bash: str = "",
        post_job_bash: str = "",
        job_is_stageable: bool = False,
        job_bypass_staging: bool = False,
        times_to_retry_job: int = 0,
    ) -> DependencyNode:
        """
        Schedule a job to run the given *python_module* on the given *parameters*.

        If this job requires other jobs to be executed first,
        include them in *depends_on*.

        This method returns a `DependencyNode` which can be used in *depends_on*
        for future jobs.

        `pre_job_bash` and `post_job_bash` are not provided as editable fields to append
        and additional job into this python job. Scoring, Post-Processing, Etc should be
        its own job. They are provided to allow for cases like 'export PYTHONPATH={path}'
        where a job expects environment variables to be set.
        """
        return self._run_python_job(
            job_name,
            python_module,
            parameters,
            depends_on=depends_on,
            resource_request=resource_request,
            override_conda_config=override_conda_config,
            category=category,
            use_pypy=use_pypy,
            container=container,
            pre_job_bash=pre_job_bash,
            post_job_bash=post_job_bash,
            job_is_stageable=job_is_stageable,
            job_bypass_staging=job_bypass_staging,
            times_to_retry_job=times_to_retry_job,
        )

    def run_python_on_args(
        self,
        job_name: Locator,
        python_module_or_path: Any,
        set_args: str,
        *,
        depends_on,
        resource_request: Optional[ResourceRequest] = None,
        override_conda_config: Optional[CondaConfiguration] = None,
        category: Optional[str] = None,
        use_pypy: bool = False,
        job_is_stageable: bool = False,
        job_bypass_staging: bool = False,
        pre_job_bash: str = "",
        post_job_bash: str = "",
        times_to_retry_job: int = 0,
        container: Optional[Container] = None,
    ) -> DependencyNode:
        """
        Schedule a job to run the given *python_script* with the given *set_args*.

        If this job requires other jobs to be executed first,
        include them in *depends_on*.

        This method returns a `DependencyNode` which can be used in *depends_on*
        for future jobs.

        `pre_job_bash` and `post_job_bash` are not provided as editable fields to append
        and additional job into this python job. Scoring, Post-Processing, Etc should be
        its own job. They are provided to allow for cases like 'export PYTHONPATH={path}'
        where a job expects environment variables to be set.
        """
        return self._run_python_job(
            job_name,
            python_module_or_path,
            set_args,
            depends_on=depends_on,
            resource_request=resource_request,
            override_conda_config=override_conda_config,
            category=category,
            use_pypy=use_pypy,
            container=container,
            pre_job_bash=pre_job_bash,
            post_job_bash=post_job_bash,
            job_is_stageable=job_is_stageable,
            job_bypass_staging=job_bypass_staging,
            times_to_retry_job=times_to_retry_job,
            treat_params_as_cmd_args=True,
        )

    def add_container(
        self,
        container_name: str,
        container_type: str,
        image: Union[str, Path],
        *,
        arguments: Optional[str] = None,
        mounts: Optional[List[str]] = None,
        image_site: Optional[str] = None,
        checksum: Optional[Mapping[str, str]] = None,
        metadata: Optional[Mapping[str, Union[float, int, str]]] = None,
        bypass_staging: bool = False,
    ) -> Container:
        """
        Add a container to the transformation catalog, to be used on a Job request

        `container_type` should be 'docker', 'singularity' or 'shifter'.

        Returns the created `Container`
        """

        if container_type not in _STR_TO_CONTAINER_TYPE:
            raise ValueError(
                f"Container Type = {container_type} is not a valid container type. Valid options are {[f'{key}, ' for key, v in _STR_TO_CONTAINER_TYPE.items()]}"
            )

        container = Container(
            container_name,
            container_type=_STR_TO_CONTAINER_TYPE[container_type],
            image=str(image.absolute()) if isinstance(image, Path) else image,
            arguments=arguments,
            mounts=mounts,
            image_site=image_site,
            checksum=immutabledict(checksum) if checksum else None,
            metadata=immutabledict(metadata) if metadata else None,
            bypass_staging=bypass_staging,
        )

        self._transformation_catalog.add_containers(container)
        return container

    def set_resource_request(self, resource_request: ResourceRequest):
        if resource_request is not None:
            resource_request = self.default_resource_request.unify(resource_request)
        else:
            resource_request = self.default_resource_request

        return resource_request

    def limit_jobs_for_category(self, category: str, max_jobs: int):
        """
        Limit the number of jobs in the given category that can run concurrently to max_jobs.
        """
        self._category_to_max_jobs[category] = max_jobs

    def _conf_limits(self) -> None:
        """
        Configure the internal Pegasus Properties dicts correctly for the category limits
        """
        for category, max_jobs in self._category_to_max_jobs.items():
            self._properties[f"dagman.{category}.maxjobs"] = str(max_jobs)

    def _nuke_checkpoints_and_clear_rc(self, output_xml_dir: Path) -> None:
        subprocess.run(
            ["python", nuke_checkpoints.__file__, output_xml_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            check=True,
        )
        self._replica_catalog.write()

    def write_dax_to_dir(self, output_xml_dir: Optional[Path] = None) -> Path:
        if not output_xml_dir:
            output_xml_dir = self._workflow_directory

        num_jobs = len(self._job_graph.jobs.keys())
        num_ckpts = len([ckpt_file for ckpt_file in output_xml_dir.rglob("___ckpt")])
        if num_jobs == num_ckpts:
            nuke = input(
                "DAX *may* create a NOOP workflow. Do you want to nuke the checkpoints and regenerate? [y/n]"
            )
            if nuke == "y":
                self._nuke_checkpoints_and_clear_rc(output_xml_dir)
                logging.info("Checkpoints cleared!")

        dax_file_name = f"{self.name}.dax"
        dax_file = output_xml_dir / dax_file_name
        logging.info("Writing DAX to %s", dax_file)
        with dax_file.open("w") as dax:
            self._job_graph.write(dax)
        build_submit_script(
            output_xml_dir / "submit.sh", dax_file_name, self._workflow_directory
        )

        # Write Out Sites Catalog
        sites_yml_path = output_xml_dir / "sites.yml"
        with sites_yml_path.open("w") as sites:
            self._sites_catalog.write(sites)
        self._properties["pegasus.catalog.site.file"] = str(sites_yml_path.absolute())

        # Write Out Replica Catalog
        replica_yml_path = output_xml_dir / "replicas.yml"
        with replica_yml_path.open("w") as replicas:
            self._replica_catalog.write(replicas)
        self._properties["pegasus.catalog.replica"] = "YAML"
        self._properties["pegasus.catalog.replica.file"] = str(
            replica_yml_path.absolute()
        )

        # Write Out Transformation Catalog
        transformation_yml_path = output_xml_dir / "transformations.yml"
        with transformation_yml_path.open("w") as transformations:
            self._transformation_catalog.write(transformations)
        self._properties["pegasus.catalog.transformation"] = "YAML"
        self._properties["pegasus.catalog.transformation.file"] = str(
            transformation_yml_path.absolute()
        )

        # Write Out Pegasus Properties
        self._conf_limits()
        pegasus_conf_path = output_xml_dir / "pegasus.properties"
        with pegasus_conf_path.open("w") as properties:
            self._properties.write(properties)

        return dax_file

    def default_conda_configuration(self) -> CondaConfiguration:
        return self._conda_script_generator.conda_config

    @_job_graph.default
    def _init_job_graph(self) -> Workflow:
        ret = Workflow(self.name)
        ret.add_metadata(name=self.name, createdby=self.created_by)
        return ret
