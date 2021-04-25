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
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union

from vistautils.parameters import Parameters

from pegasus_wrapper.artifact import DependencyNode
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.resource_request import ResourceRequest
from pegasus_wrapper.version import version as __version__  # noqa
from pegasus_wrapper.workflow import WorkflowBuilder

from Pegasus.api import Container
from saga_tools.conda import CondaConfiguration

_SINGLETON_WORKFLOW_BUILDER: WorkflowBuilder = None  # type: ignore


def initialize_vista_pegasus_wrapper(parameters: Parameters) -> None:
    global _SINGLETON_WORKFLOW_BUILDER  # pylint:disable=global-statement
    _SINGLETON_WORKFLOW_BUILDER = WorkflowBuilder.from_parameters(parameters)


def _assert_singleton_workflow_builder() -> None:
    if not _SINGLETON_WORKFLOW_BUILDER:
        raise RuntimeError(
            "You must call initialize_vista_pegasus_wrapper(params) "
            "before calling any other wrapper functions."
        )


def directory_for(locator: Locator) -> Path:
    """
    Get the suggested working/output directory
    for a job with the given `Locator`.
    """
    _assert_singleton_workflow_builder()
    return _SINGLETON_WORKFLOW_BUILDER.directory_for(locator)


def run_python_on_parameters(
    job_name: Locator,
    python_module: Any,
    parameters: Union[Parameters, Dict[str, Any]],
    *,
    depends_on,
    resource_request: Optional[ResourceRequest] = None,
    override_conda_config: Optional[CondaConfiguration] = None,
    category: Optional[str] = None,
    container=None,
    use_pypy: bool = False,
    pre_job_bash: str = "",
    post_job_bash: str = "",
    times_to_retry_job: int = 0,
) -> DependencyNode:
    """
    Schedule a job to run the given *python_module* on the given *parameters*.

    If this job requires other jobs to be executed first,
    include them in *depends_on*.

    This method returns a `DependencyNode` which can be used in *depends_on*
    for future jobs.
    """
    _assert_singleton_workflow_builder()
    return _SINGLETON_WORKFLOW_BUILDER.run_python_on_parameters(
        job_name=job_name,
        python_module=python_module,
        parameters=parameters,
        depends_on=depends_on,
        resource_request=resource_request,
        override_conda_config=override_conda_config,
        category=category,
        use_pypy=use_pypy,
        container=container,
        pre_job_bash=pre_job_bash,
        post_job_bash=post_job_bash,
        times_to_retry_job=times_to_retry_job,
    )


def limit_jobs_for_category(category: str, max_jobs: int):
    """
    Limit the number of jobs in the given category that can run concurrently to max_jobs.
    """
    _assert_singleton_workflow_builder()
    return _SINGLETON_WORKFLOW_BUILDER.limit_jobs_for_category(category, max_jobs)


def run_python_on_args(
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
) -> DependencyNode:
    """
    Schedule a job to run the given *python_script* with the given *set_args*.

    If this job requires other jobs to be executed first,
    include them in *depends_on*.

    This method returns a `DependencyNode` which can be used in *depends_on*
    for future jobs.
    """
    _assert_singleton_workflow_builder()
    return _SINGLETON_WORKFLOW_BUILDER.run_python_on_args(
        job_name=job_name,
        python_module_or_path=python_module_or_path,
        set_args=set_args,
        depends_on=depends_on,
        resource_request=resource_request,
        override_conda_config=override_conda_config,
        category=category,
        use_pypy=use_pypy,
        job_is_stageable=job_is_stageable,
        job_bypass_staging=job_bypass_staging,
        pre_job_bash=pre_job_bash,
        post_job_bash=post_job_bash,
        times_to_retry_job=times_to_retry_job,
    )


def default_conda_configuration() -> CondaConfiguration:
    _assert_singleton_workflow_builder()
    return _SINGLETON_WORKFLOW_BUILDER.default_conda_configuration()


def write_workflow_description(output_xml_dir: Optional[Path] = None) -> Path:
    _assert_singleton_workflow_builder()
    return _SINGLETON_WORKFLOW_BUILDER.write_dax_to_dir(output_xml_dir)


def add_container(
    container_name: str,
    container_type: str,
    image: str,
    *,
    arguments: Optional[str] = None,
    mounts: Optional[List[str]] = None,
    image_site: Optional[str] = None,
    checksum: Optional[Mapping[str, str]] = None,
    metadata: Optional[Mapping[str, Union[float, int, str]]] = None,
    bypass_staging: bool = False,
) -> Container:
    _assert_singleton_workflow_builder()
    return _SINGLETON_WORKFLOW_BUILDER.add_container(
        container_name,
        container_type,
        image,
        arguments=arguments,
        mounts=mounts,
        image_site=image_site,
        checksum=checksum,
        metadata=metadata,
        bypass_staging=bypass_staging,
    )


def experiment_directory() -> Path:
    """
    The directory to which the Pegasus DAX for this experiment will be written.
    Typically all experiment outputs will be written within this directory as well.
    """
    _assert_singleton_workflow_builder()
    return (
        _SINGLETON_WORKFLOW_BUILDER._workflow_directory  # pylint:disable=protected-access
    )
