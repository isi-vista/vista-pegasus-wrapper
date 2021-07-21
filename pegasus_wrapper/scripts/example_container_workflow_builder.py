# pragma: no cover
from pathlib import Path
from random import Random

from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point

from pegasus_wrapper import (
    add_container,
    initialize_vista_pegasus_wrapper,
    run_bash,
    run_python_on_args,
    run_python_on_parameters,
    start_docker_as_service,
    stop_docker_as_service,
    write_workflow_description,
)
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.pegasus_profile import PegasusProfile
from pegasus_wrapper.resource_request import SlurmResourceRequest
from pegasus_wrapper.scripts import sort_nums_in_file

# NOTES: We can confirm that job is running in container by checking:
# python -V
# /nas/gaia/users/napiersk/archives/docker/python-3-6-3.tar
# <profile namespace="pegasus" key="glite.arguments">--nodelist saga31</profile>
# pursuit_resource_request:
#    exclude_list: "saga01,saga02,saga03,saga04,saga05,saga06,saga07,saga08,saga10, /
# saga11,saga12,saga13,saga14,saga15,saga16,saga17,saga18,saga19,saga20, /
# saga21,saga22,saga23,saga24,saga25,saga26,gaia01,gaia02"
#    partition: ephemeral
# SlurmResourceRequest.from_parameters({"run_on_single_node": "saga31"})


def example_workflow(params: Parameters):  # pragma: no cover
    """
    An example script to generate a container workflow for submission to Pegasus.
    """
    tmp_path = params.creatable_directory("example_root_dir")
    docker_tar = params.creatable_file("docker_tar")
    docker_build_dir = params.existing_directory("docker_build_dir")
    docker_image_name = params.string(
        "docker_image_name", default="pegasus_wrapper_container_demo"
    )
    docker_image_tag = params.string("docker_image_tag", default="0.2")
    mongo_db_tar = params.string(
        "mongo_db_tar", default="/nas/gaia/shared/cluster/docker/mongo-4.4.tar"
    )
    monogo_db_data = "/scratch/dockermount/pegasus_wrapper_tmp/data"
    mongo_db_config = "/scratch/dockermount/pegasus_wrapper_tmp/config"

    # Generating parameters for initializing a workflow
    # We recommend making workflow directory, site, and partition parameters
    # in an research workflow
    workflow_params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_directory": str(tmp_path / "working"),
            "site": "saga",
            "namespace": "test",
            "home_dir": str(tmp_path),
            "partition": "scavenge",
        }
    )

    saga31_request = SlurmResourceRequest.from_parameters(
        Parameters.from_mapping({"run_on_single_node": "saga31", "partition": "gaia"})
    )

    workflow_params = workflow_params.unify(params)

    # Our source input for the sample jobs
    input_file = tmp_path / "raw_nums.txt"
    add_y_output_file_nas = tmp_path / "nums_y.txt"
    sorted_output_file_nas = tmp_path / "sorted.txt"

    random = Random()
    random.seed(0)
    nums = [int(random.random() * 100) for _ in range(0, 25)]

    # Base Job Locator
    job_locator = Locator(("jobs",))
    docker_python_root = Path("/home/app/")

    job_profile = PegasusProfile(
        namespace="pegasus", key="transfer.bypass.input.staging", value="True"
    )

    # Write a list of numbers out to be able to run the workflow
    with input_file.open("w") as mult_file:
        mult_file.writelines(f"{num}\n" for num in nums)

    initialize_vista_pegasus_wrapper(workflow_params)

    build_container = run_bash(
        job_locator / "build_docker",
        command=[
            "mkdir -p /scratch/dockermount/pegasus_wrapper_tmp",
            f"cd {docker_build_dir}",
            f"docker build . -t {docker_image_name}:{docker_image_tag}",
            f"docker save -o /scratch/dockermount/pegasus_wrapper_tmp/{docker_tar.name} {docker_image_name}:{docker_image_tag}",
            f"cp /scratch/dockermount/pegasus_wrapper_tmp/{docker_tar.name} {docker_tar.absolute()}",
            f"chmod go+r {docker_tar.absolute()}",
            f"docker load --input {mongo_db_tar}",
            f"mkdir -p {monogo_db_data}",
            f"mkdir -p {mongo_db_config}",
        ],
        depends_on=[],
        resource_request=saga31_request,
    )

    python36 = add_container(
        f"{docker_image_name}:{docker_image_tag}",
        "docker",
        str(docker_tar.absolute()),
        image_site="saga",
        bypass_staging=True,
    )

    mongo4_4 = add_container(
        "mongo:4.4", "docker", mongo_db_tar, image_site="saga", bypass_staging=True
    )

    start_mongo = start_docker_as_service(
        mongo4_4,
        depends_on=[build_container],
        mounts=[f"{monogo_db_data}:/data/db", f"{mongo_db_config}/etc/custom"],
        docker_args=f"-p 27017:27017",
        resource_request=saga31_request,
    )

    add_y_job = run_python_on_args(
        job_locator / "add",
        docker_python_root / "add_y.py",
        set_args=f"{input_file} {add_y_output_file_nas} --y 10",
        depends_on=[build_container],
        job_profiles=[job_profile],
        resource_request=saga31_request,
        container=python36,
        input_file_paths=[input_file],
        output_file_paths=[add_y_output_file_nas],
    )

    sort_job = run_python_on_parameters(
        job_locator / "sort",
        sort_nums_in_file,
        {"input_file": add_y_output_file_nas, "output_file": sorted_output_file_nas},
        depends_on=[add_y_job],
        container=python36,
        job_profiles=[job_profile],
        resource_request=saga31_request,
        input_file_paths=add_y_output_file_nas,
        output_file_paths=sorted_output_file_nas,
    )

    _ = stop_docker_as_service(
        mongo4_4, depends_on=[start_mongo, sort_job], resource_request=saga31_request
    )

    # Generate the Pegasus DAX file & a Submit Script
    write_workflow_description(tmp_path)


if __name__ == "__main__":
    parameters_only_entry_point(example_workflow)
