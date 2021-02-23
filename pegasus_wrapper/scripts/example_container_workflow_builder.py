# pragma: no cover
from random import Random

from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point

from pegasus_wrapper import (
    add_container,
    initialize_vista_pegasus_wrapper,
    run_python_on_parameters,
    write_workflow_description,
)
from pegasus_wrapper.artifact import ValueArtifact
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.resource_request import SlurmResourceRequest
from pegasus_wrapper.scripts import multiply_by_x, sort_nums_in_file

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
    multiply_input_file = tmp_path / "raw_nums.txt"

    random = Random()
    random.seed(0)
    nums = [int(random.random() * 100) for _ in range(0, 25)]

    multiply_output_file = tmp_path / "multiplied_nums.txt"
    sorted_output_file = tmp_path / "sorted_nums.txt"

    # Base Job Locator
    job_locator = Locator(("jobs",))

    # Write a list of numbers out to be able to run the workflow
    with multiply_input_file.open("w") as mult_file:
        mult_file.writelines(f"{num}\n" for num in nums)

    initialize_vista_pegasus_wrapper(workflow_params)

    add_container(
        "python3.6",
        "docker",
        "/nas/gaia/users/napiersk/archives/docker/python-3-6-3.tar",
        image_site="saga",
    )

    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=run_python_on_parameters(
            job_locator / "multiply",
            multiply_by_x,
            {
                "input_file": multiply_input_file,
                "output_file": multiply_output_file,
                "x": 4,
                "logfile": str(tmp_path / "multiply_log.txt"),
            },
            depends_on=[],
            container="python3.6",
            resource_request=saga31_request,
        ),
        locator=Locator("multiply"),
    )

    run_python_on_parameters(
        job_locator / "sort",
        sort_nums_in_file,
        {"input_file": multiply_output_file, "output_file": sorted_output_file},
        depends_on=[multiply_artifact],
        container="python3.6",
        resource_request=saga31_request
        # if you want to use a different resource for some task, you can do this way
        # resource_request=SlurmResourceRequest.from_parameters(slurm_params),
    )

    # Generate the Pegasus DAX file & a Submit Script
    write_workflow_description(tmp_path)


if __name__ == "__main__":
    parameters_only_entry_point(example_workflow)
