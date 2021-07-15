# pragma: no cover
from pathlib import Path
from random import Random

from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point

from pegasus_wrapper import (
    add_container,
    initialize_vista_pegasus_wrapper,
    run_bash,
    run_container,
    run_python_on_args,
    run_python_on_parameters,
    write_workflow_description,
)
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.pegasus_profile import PegasusProfile
from pegasus_wrapper.resource_request import SlurmResourceRequest
from pegasus_wrapper.scripts import add_y, sort_nums_in_file

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
    An example script to generate a workflow for submission to Pegasus.
    """
    tmp_path = params.creatable_directory("example_root_dir")
    docker_shared_volume = params.string("docker_shared_volume", default="/data/")
    docker_nas_volume = params.string(
        "docker_nas_volume", default="/scratch/dockermount/pegasus_wrapper_demo"
    )
    docker_tar = params.existing_file("docker_tar")

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
    input_file_docker_scratch = docker_nas_volume + "/raw_nums.txt"
    input_file_docker = docker_shared_volume + "/raw_nums.txt"

    random = Random()
    random.seed(0)
    nums = [int(random.random() * 100) for _ in range(0, 25)]

    add_y_output_file_docker = docker_shared_volume + "/nums_y.txt"
    sorted_output_file_docker = docker_shared_volume + "/sorted.txt"

    add_y_output_file_docker_scratch = docker_nas_volume + "/nums_y.txt"
    sorted_output_file_docker_scratch = docker_nas_volume + "/sorted.txt"

    add_y_output_file_nas = tmp_path / "nums_y.txt"
    sorted_output_file_nas = tmp_path / "sorted.txt"

    # Base Job Locator
    job_locator = Locator(("jobs",))
    docker_python_root = Path("/home/app/")

    # Write a list of numbers out to be able to run the workflow
    with input_file.open("w") as mult_file:
        mult_file.writelines(f"{num}\n" for num in nums)

    initialize_vista_pegasus_wrapper(workflow_params)

    # Useful for keeping in mind all the container information
    # Need to see if the version number should be somewhere other
    # Than the name for Pegasus, may look into making a custom class
    # for use in the wrapper for job configuration defaults
    python36 = add_container(
        "pegasus_wrapper_container_demo:0.1",
        "docker",
        str(docker_tar.absolute()),
        image_site="saga",
        bypass_staging=True,
        mounts=[f"{docker_nas_volume}:{docker_shared_volume}"],
        resource_request=saga31_request,
    )

    job_profile = PegasusProfile(
        namespace="pegasus", key="transfer.bypass.input.staging", value="True"
    )

    # I'm not a fan of this at the moment, it feels like it required a lot
    # Of dev knowledge to use, which is against the idea of the wrapper
    # Better would be converting a python job call into this latout
    # These also require quite a bit of bash in addition to just the job
    # To work properly and get outputs back to the nas from scratch
    add_y_job = run_container(
        job_locator / "add",
        "pegasus_wrapper_container_demo:0.1",
        f"--rm -v {docker_nas_volume}:{docker_shared_volume}",
        f"/usr/local/bin/python {docker_python_root / 'add_y.py'} {input_file_docker} {add_y_output_file_docker} --y 10",
        depends_on=[],
        job_profiles=[job_profile],
        resource_request=saga31_request,
        category="container",
        post_job_bash="\n".join(
            [
                f"cp {add_y_output_file_docker_scratch} {add_y_output_file_nas}",
                # f"cp {sorted_output_file_docker_scratch} {sorted_output_file_nas}",
            ]
        ),
        pre_job_bash="\n".join(
            [
                f"mkdir -p /scratch/dockermount/pegasus_wrapper_demo",
                f"cp {input_file} {input_file_docker_scratch}",
            ]
        ),
    )

    # sort_job = run_python_on_parameters(
    #     job_locator / "sort",
    #     sort_nums_in_file,
    #     {
    #         "input_file": add_y_output_file_docker,
    #         "output_file": sorted_output_file_docker,
    #     },
    #     depends_on=[add_y_job],
    #     container=python36,
    #     job_profiles=[job_profile],
    #     resource_request=saga31_request,
    # )

    # Generate the Pegasus DAX file & a Submit Script
    write_workflow_description(tmp_path)


if __name__ == "__main__":
    parameters_only_entry_point(example_workflow)
