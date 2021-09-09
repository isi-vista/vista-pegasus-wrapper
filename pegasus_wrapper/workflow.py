"""
Core workflow management classes for the Vista Pegasus wrapper.

The user should typically not refer to these classes directly
and should instead use the methods in the root of the package.
"""
import logging
import subprocess
from itertools import chain
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple, Union

from attr import attrib, attrs
from attr.validators import instance_of, optional

from immutablecollections import immutabledict, immutableset
from vistautils.class_utils import fully_qualified_name
from vistautils.io_utils import CharSink
from vistautils.parameters import Parameters, YAMLParametersWriter

from pegasus_wrapper.artifact import DependencyNode, _canonicalize_depends_on
from pegasus_wrapper.conda_job_script import CondaJobScriptGenerator
from pegasus_wrapper.docker_job_script import DockerJobScriptGenerator
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.pegasus_container import PegasusContainerFile
from pegasus_wrapper.pegasus_profile import PegasusProfile
from pegasus_wrapper.pegasus_transformation import PegasusTransformation
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

# Default paths used for various python -> docker coordination
DOCKERMOUNT_SCRATCH_PATH_ROOT = Path("/scratch/dockermount/")
BASH_EXECUTABLE_PATH = Path("/usr/bin/bash")
PYTHON_EXECUTABLE_DOCKER_PATH = Path("/usr/local/bin/python")
DOCKER_MOUNT_ROOT = Path("/data/")

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
    _data_configuration: str = attrib(validator=instance_of(str), kw_only=True)
    _default_site: str = attrib(validator=instance_of(str), kw_only=True)
    default_resource_request: ResourceRequest = attrib(
        validator=instance_of(ResourceRequest), kw_only=True
    )
    _conda_script_generator: Optional[CondaJobScriptGenerator] = attrib(
        validator=optional(instance_of(CondaJobScriptGenerator)),
        default=None,
        kw_only=True,
    )
    _docker_script_generator: Optional[DockerJobScriptGenerator] = attrib(
        validator=optional(instance_of(DockerJobScriptGenerator)),
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
    _transformation_name_to_transformations: Dict[
        str, List[PegasusTransformation]
    ] = attrib(kw_only=True, factory=dict)
    # In order to use docker images as a service during a workflow we need
    # to be able to configure the dependent jobs when we go to write-out the workflow
    _container_to_start_stop_job: Dict[Container, Tuple[Job, Job]] = attrib(
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
            docker_script_generator=DockerJobScriptGenerator.from_parameters(params),
            namespace=params.string("namespace"),
            default_resource_request=ResourceRequest.from_parameters(params),
            data_configuration=params.string("data_configuration", default="sharedfs"),
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

    def _define_transformation(
        self,
        name: str,
        pfn: str,
        *,
        namespace: Optional[str] = None,
        version: Optional[str] = None,
        site: Optional[str] = None,
        is_stageable: bool = False,
        bypass_staging: bool = True,
        container: Optional[Container] = None,
        arch: Optional[Arch] = None,
        os_type: Optional[OS] = None,
    ) -> PegasusTransformation:
        # Try to see if we have the target transformation already made
        for transformation in self._transformation_name_to_transformations.get(
            name, immutableset()
        ):
            if transformation.container == container:
                return transformation
        # Otherwise make the transformation and return it
        transform = Transformation(
            name,
            namespace,
            version,
            site if site is not None else self._default_site,
            pfn,
            is_stageable=is_stageable,
            bypass_staging=bypass_staging,
            container=container,
            arch=arch,
            os_type=os_type,
        )

        self._transformation_catalog.add_transformations(transform)

        pegasus_transform = PegasusTransformation(
            name=name, transformation=transform, container=container
        )

        if name not in self._transformation_name_to_transformations:
            self._transformation_name_to_transformations[name] = list()

        self._transformation_name_to_transformations[name].append(pegasus_transform)
        return pegasus_transform

    def _update_job_settings(
        self,
        category: str,
        checkpoint_path: Path,
        ckpt_name: Locator,
        depends_on,
        job: Job,
        job_name: Locator,
        job_profiles: Iterable[PegasusProfile],
        resource_request: ResourceRequest,
        times_to_retry_job: int,
    ) -> DependencyNode:
        """
        Apply a variety of shared settings to a job.

        Centralized logic for multiple job types to use.
        """
        self._job_graph.add_jobs(job)

        # Configure SLURM resource request
        resource_request.apply_to_job(job, job_name=self._job_name_for(job_name))

        # Set the DAGMAN category to potentially limit the number of active jobs
        job.add_dagman_profile(category=category, retry=str(times_to_retry_job))

        # Apply other user defined pegasus profiles
        for profile in job_profiles:
            job.add_profiles(profile.namespace, key=profile.key, value=profile.value)

        # Handle depedent job additions from the `depends_on` variable
        for parent_dependency in depends_on:
            if parent_dependency.job:
                self._job_graph.add_dependency(job, parents=[parent_dependency.job])
            for out_file in parent_dependency.output_files:
                job.add_inputs(out_file)

        # Handle Output Files
        # This is currently only handled as the checkpoint file
        # See: https://github.com/isi-vista/vista-pegasus-wrapper/issues/25
        # If the checkpoint file already exists, we want to add it to the replica catalog
        # so that we don't run the job corresponding to the checkpoint file again
        checkpoint_pegasus_file = self.create_file(
            f"{ckpt_name}", checkpoint_path, add_to_catalog=checkpoint_path.exists()
        )
        job.add_outputs(checkpoint_pegasus_file, stage_out=False)

        return DependencyNode.from_job(job, output_files=[checkpoint_pegasus_file])

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
        job_profiles: Iterable[PegasusProfile] = immutableset(),
        treat_params_as_cmd_args: bool = False,
        input_file_paths: Union[Iterable[Union[Path, str]], Path, str] = immutableset(),
        output_file_paths: Union[Iterable[Union[Path, str]], Path, str] = immutableset(),
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

        if container:
            return self._run_python_in_container(
                job_name,
                computed_module_or_path,
                args_or_params,
                container,
                depends_on=depends_on,
                input_files=input_file_paths,
                output_files=output_file_paths,
                resource_request=resource_request,
                category=category,
                pre_docker_bash=pre_job_bash,
                post_docker_bash=post_job_bash,
                job_is_stageable=job_is_stageable,
                job_bypass_staging=job_bypass_staging,
                times_to_retry_job=times_to_retry_job,
                job_profiles=job_profiles,
            )

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
        resource_request = self.set_resource_request(resource_request)

        job = Job(script_executable)
        dependency_node = self._update_job_settings(
            category,
            checkpoint_path,
            ckpt_name,
            depends_on,
            job,
            job_name,
            job_profiles,
            resource_request,
            times_to_retry_job,
        )
        self._signature_to_job[signature] = dependency_node

        logging.info("Scheduled Python job %s", job_name)
        return dependency_node

    def _run_python_in_container(
        self,
        job_name: Locator,
        python_module_or_path_on_docker: Union[str, Path],
        python_args_or_parameters: Union[Parameters, str],
        container: Container,
        *,
        depends_on,
        docker_args: str = "",
        python_executable_path_in_docker: Path = PYTHON_EXECUTABLE_DOCKER_PATH,
        input_files: Union[Iterable[Union[Path, str]], Path, str] = immutableset(),
        output_files: Union[Iterable[Union[Path, str]], Path, str] = immutableset(),
        docker_mount_root: Path = DOCKER_MOUNT_ROOT,
        resource_request: Optional[ResourceRequest] = None,
        category: Optional[str] = None,
        pre_docker_bash: Union[Iterable[str], str] = "",
        post_docker_bash: Union[Iterable[str], str] = "",
        job_is_stageable: bool = False,
        job_bypass_staging: bool = False,
        times_to_retry_job: int = 0,
        job_profiles: Iterable[PegasusProfile] = immutableset(),
    ) -> DependencyNode:
        """
        Automatically converts a python job into a container request
        """
        # Ensure the input and output files are iterables of Path or str
        if isinstance(input_files, (Path, str)):
            input_files = immutableset([input_files])
        if isinstance(output_files, (Path, str)):
            output_files = immutableset([output_files])
        # A set to keep track of all the file names that will be created or copied into
        # The mounted directory. We use this to raise errors if a duplicate name would appear
        params_file_name = "____params.params"
        params_file = None
        file_names = set(params_file_name)
        job_dir = self.directory_for(job_name)
        # Define the root mount point for scratch mount
        scratch_root = DOCKERMOUNT_SCRATCH_PATH_ROOT / self.name / str(job_name)
        # Define the self-needed docker args
        modified_docker_args = (
            f"--rm -v {scratch_root}:{docker_mount_root} " + docker_args
        )

        # Build paths mappings for docker
        mapping_input_files = []
        for i_file in input_files:
            if i_file.name in file_names:
                raise RuntimeError(
                    f"Unable to create container job {job_name} with multiple files with name {i_file.name}"
                )
            file_names.add(i_file.name)
            mapping_input_files.append(
                (
                    str(i_file.absolute()),
                    PegasusContainerFile(
                        name=i_file.name,
                        nas=i_file,
                        scratch=scratch_root / i_file.name,
                        docker=docker_mount_root / i_file.name,
                    ),
                )
            )
        converted_input_files = immutabledict(mapping_input_files)

        mapping_output_files = []
        for o_file in output_files:
            if o_file.name in file_names:
                raise RuntimeError(
                    f"Unable to create container job {job_name} with multiple files with name {o_file.name}"
                )
            file_names.add(o_file.name)
            mapping_output_files.append(
                (
                    str(o_file.absolute()),
                    PegasusContainerFile(
                        name=o_file.name,
                        nas=o_file,
                        scratch=scratch_root / o_file.name,
                        docker=docker_mount_root / o_file.name,
                    ),
                )
            )
        converted_output_files = immutabledict(mapping_output_files)

        # Process the Python Parameters or Args for any file paths which need to change
        if isinstance(python_args_or_parameters, Parameters):
            mutable_params = dict(python_args_or_parameters.as_mapping())
            for key, value in python_args_or_parameters.as_mapping().items():
                if isinstance(value, Path):
                    if str(value.absolute()) in converted_input_files:
                        mutable_params[key] = str(
                            converted_input_files[str(value.absolute())].docker.absolute()
                        )
                    elif str(value.absolute()) in converted_output_files:
                        mutable_params[key] = str(
                            converted_output_files[
                                str(value.absolute())
                            ].docker.absolute()
                        )
            modified_params = Parameters.from_mapping(mutable_params)
            params_path = job_dir / params_file_name
            YAMLParametersWriter().write(modified_params, CharSink.to_file(params_path))
            params_file = PegasusContainerFile(
                name=params_file_name,
                nas=params_path,
                scratch=scratch_root / params_file_name,
                docker=docker_mount_root / params_file_name,
            )
            python_args = params_file.docker
        elif isinstance(python_args_or_parameters, str):
            python_args_tok = []
            for tok in python_args_or_parameters.split(" "):
                if tok in converted_input_files:
                    python_args_tok.append(
                        str(converted_input_files[tok].docker.absolute())
                    )
                elif tok in converted_output_files:
                    python_args_tok.append(
                        str(converted_output_files[tok].docker.absolute())
                    )
                else:
                    python_args_tok.append(tok)
            python_args = " ".join(python_args_tok)
        else:
            raise RuntimeError(
                f"Cannot handle python_args_or_parameters of type {type(python_args_or_parameters)}. Data: {python_args_or_parameters}"
            )

        # Combine any user requested pre-docker bash with automatic
        # Movement of files from NAS locations to /scratch dir locations
        pre_job_bash = "\n".join(
            chain(
                [
                    f"mkdir -p {scratch_root}",
                    f"cp {str(params_file.nas.absolute())} {str(params_file.scratch.absolute())}"
                    if params_file
                    else "",
                ],
                [
                    f"cp {str(i_file.nas.absolute())} {str(i_file.scratch.absolute())}"
                    for i_file in converted_input_files.values()
                ],
                pre_docker_bash,
            )
        )

        # Combine any user requested post-docker bash with automatic
        # Movement of files from /scratch locations to NAS locations
        post_job_bash = "\n".join(
            chain(
                [
                    f"cp {str(o_file.scratch.absolute())} {str(o_file.nas.absolute())}"
                    for o_file in converted_output_files.values()
                ],
                post_docker_bash,
            )
        )

        # Generate the command to run the python job
        python_start = (
            f"-m {python_module_or_path_on_docker}"
            if isinstance(python_module_or_path_on_docker, str)
            else str(python_module_or_path_on_docker)
        )
        docker_run_command = (
            f"{python_executable_path_in_docker} {python_start} {python_args}"
        )

        return self.run_container(
            job_name,
            container.name,
            modified_docker_args,
            docker_run_command,
            container.image,
            depends_on=depends_on,
            job_is_stageable=job_is_stageable,
            job_bypass_staging=job_bypass_staging,
            times_to_retry_job=times_to_retry_job,
            job_profiles=job_profiles,
            pre_job_bash=pre_job_bash,
            post_job_bash=post_job_bash,
            category=category,
            resource_request=resource_request,
        )

    def run_container(
        self,
        job_name: Locator,
        docker_image_name: str,
        docker_args: str,
        docker_run_comand: str,
        docker_tar_path: str,
        *,
        depends_on,
        resource_request: Optional[ResourceRequest] = None,
        category: Optional[str] = None,
        pre_job_bash: str = "",
        post_job_bash: str = "",
        job_is_stageable: bool = False,
        job_bypass_staging: bool = True,
        times_to_retry_job: int = 0,
        job_profiles: Iterable[PegasusProfile] = immutableset(),
    ) -> DependencyNode:

        job_dir = self.directory_for(job_name)
        ckpt_name = job_name / "___ckpt"
        checkpoint_path = job_dir / "___ckpt"
        depends_on = _canonicalize_depends_on(depends_on)

        signature = (docker_image_name, docker_args)
        if signature in self._signature_to_job:
            logging.info("Job %s recognized as a duplicate", job_name)
            return self._signature_to_job[signature]

        script_path = job_dir / "___run.sh"

        # Part of one strategy to run a container through a bash script
        self._docker_script_generator.write_shell_script_to(
            docker_image_name=docker_image_name,
            docker_command=docker_run_comand,
            docker_tar_path=docker_tar_path,
            working_directory=job_dir,
            script_path=script_path,
            cmd_args=docker_args,
            ckpt_path=checkpoint_path,
            pre_job=pre_job_bash,
            post_job=post_job_bash,
        )

        # TODO - Refactor this so it uses the BASH transformation to form a job
        # With the script path as an argument
        # https://github.com/isi-vista/vista-pegasus-wrapper/issues/103
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
        )

        self._transformation_catalog.add_transformations(script_executable)
        resource_request = self.set_resource_request(resource_request)

        job = Job(script_executable)
        dependency_node = self._update_job_settings(
            category,
            checkpoint_path,
            ckpt_name,
            depends_on,
            job,
            job_name,
            job_profiles,
            resource_request,
            times_to_retry_job,
        )
        self._signature_to_job[signature] = dependency_node

        logging.info("Scheduled Docker job %s", job_name)
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
        job_profiles: Iterable[PegasusProfile] = immutableset(),
        input_file_paths: Union[Iterable[Union[Path, str]], Path, str] = immutableset(),
        output_file_paths: Union[Iterable[Union[Path, str]], Path, str] = immutableset(),
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
            job_profiles=job_profiles,
            input_file_paths=input_file_paths,
            output_file_paths=output_file_paths,
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
        job_profiles: Iterable[PegasusProfile] = immutableset(),
        input_file_paths: Union[Iterable[Union[Path, str]], Path, str] = immutableset(),
        output_file_paths: Union[Iterable[Union[Path, str]], Path, str] = immutableset(),
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
            job_profiles=job_profiles,
            treat_params_as_cmd_args=True,
            input_file_paths=input_file_paths,
            output_file_paths=output_file_paths,
        )

    def run_bash(
        self,
        job_name: Locator,
        command: Union[Iterable[str], str],
        *,
        depends_on,
        resource_request: Optional[ResourceRequest] = None,
        category: Optional[str] = None,
        job_is_stageable: bool = False,
        job_bypass_staging: bool = False,
        times_to_retry_job: int = 0,
        job_profiles: Iterable[PegasusProfile] = immutableset(),
        container: Optional[Container] = None,
        path_to_bash: Path = BASH_EXECUTABLE_PATH,
    ) -> DependencyNode:
        """
        Schedule a job to run the given *command* with the given *resource_request*

        If this job requires other jobs to be executed first,
        include them in *depends_on*.

        This method returns a `DependencyNode` which can be used in *depends_on*
        for future jobs.
        """
        if isinstance(command, str):
            command = [command]

        commands_hashable = immutableset(command)

        signature = (job_name, commands_hashable)
        if signature in self._signature_to_job:
            logging.info("Job %s recognized as duplicate", job_name)
            return self._signature_to_job[signature]

        depends_on = _canonicalize_depends_on(depends_on)

        bash_transform = self._define_transformation(
            "bash",
            str(path_to_bash.absolute()),
            site=self._default_site,
            container=container,
            is_stageable=job_is_stageable,
            bypass_staging=job_bypass_staging,
        ).transformation

        job_dir = self.directory_for(job_name)
        ckpt_name = job_name / "___ckpt"
        ckpt_path = job_dir / "___ckpt"
        job_script = job_dir / "script.sh"

        commands_with_ckpt = list(command)
        commands_with_ckpt.append(f"touch {ckpt_path.absolute()}")
        commands_with_ckpt.insert(0, f"cd {job_dir}")

        job_script.write_text("\n".join(commands_with_ckpt))
        resource_request = self.set_resource_request(resource_request)

        bash_job = Job(bash_transform)
        bash_job.add_args(str(job_script.absolute()))
        dependency_node = self._update_job_settings(
            category,
            ckpt_path,
            ckpt_name,
            depends_on,
            bash_job,
            job_name,
            job_profiles,
            resource_request,
            times_to_retry_job,
        )

        self._signature_to_job[signature] = dependency_node
        logging.info("Scheduled bash job %s", job_name)

        return dependency_node

    def start_docker_as_service(
        self,
        container: Container,
        *,
        depends_on,
        mounts: Union[Iterable[str], str] = immutableset(),
        docker_args: str = "",
        resource_request: Optional[ResourceRequest] = None,
    ) -> DependencyNode:
        """
        Start a docker image as a service
        """
        if isinstance(mounts, str):
            mounts = immutableset(mounts)

        container_loc = Locator(("containers", container.name))
        container_dir = self.directory_for(container_loc)
        container_start_path = container_dir / "start.sh"
        container_stop_path = container_dir / "stop.sh"

        docker_args += " -v ".join(mounts)

        self._docker_script_generator.write_service_shell_script_to(
            container.name,
            docker_image_path=container.image,
            docker_args=docker_args,
            start_script_path=container_start_path,
            stop_script_path=container_stop_path,
        )

        return self.run_bash(
            container_loc / "start",
            str(container_start_path.absolute()),
            resource_request=resource_request,
            depends_on=depends_on,
        )

    def stop_docker_as_service(
        self,
        container: Container,
        *,
        depends_on,
        resource_request: Optional[ResourceRequest] = None,
    ) -> DependencyNode:
        """
        Stops a docker image from running as a service.

        Must be provided an identical resource request as the start service.
        """
        container_loc = Locator(("containers", container.name))
        container_dir = self.directory_for(container_loc)
        container_stop_path = container_dir / "stop.sh"

        if not container_stop_path.exists():
            raise RuntimeError(
                f"This docker container was never scheduled to start as a service. Name {container.name}"
            )

        return self.run_bash(
            container_loc / "stop",
            str(container_stop_path.absolute()),
            resource_request=resource_request,
            depends_on=depends_on,
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
            image_site=image_site if image_site is not None else self._default_site,
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
