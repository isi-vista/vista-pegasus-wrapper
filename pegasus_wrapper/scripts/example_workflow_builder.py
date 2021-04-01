# pragma: no cover
from random import Random

from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point

from pegasus_wrapper import (
    initialize_vista_pegasus_wrapper,
    run_python_on_args,
    run_python_on_parameters,
    write_workflow_description,
)
from pegasus_wrapper.artifact import ValueArtifact
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.scripts import add_y, multiply_by_x, sort_nums_in_file


def example_workflow(params: Parameters):
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

    workflow_params = workflow_params.unify(params)

    # Our source input for the sample jobs
    multiply_input_file = tmp_path / "raw_nums.txt"

    random = Random()
    random.seed(0)
    nums = [int(random.random() * 100) for _ in range(0, 25)]

    multiply_output_file = tmp_path / "multiplied_nums.txt"
    sorted_output_file = tmp_path / "sorted_nums.txt"
    add_output_file = tmp_path / "add_nums.txt"

    # Base Job Locator
    job_locator = Locator(("jobs",))

    # Write a list of numbers out to be able to run the workflow
    with multiply_input_file.open("w") as mult_file:
        mult_file.writelines(f"{num}\n" for num in nums)

    initialize_vista_pegasus_wrapper(workflow_params)

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
        ),
        locator=Locator("multiply"),
    )

    # You can also just track the dep node itself to pass to a future job if you don't
    # need the value portion of an artifacy
    mul_dep = run_python_on_parameters(
        job_locator / "sort",
        sort_nums_in_file,
        {"input_file": multiply_output_file, "output_file": sorted_output_file},
        depends_on=[multiply_artifact],
        # if you want to use a different resource for some task, you can do this way
        # resource_request=SlurmResourceRequest.from_parameters(slurm_params),
    )

    run_python_on_args(
        job_locator / "add",
        add_y,
        set_args=f"{sorted_output_file} {add_output_file} --y 10",
        depends_on=[mul_dep],
        category="add",  # Can be used as a custom category for job limits
    )

    # If you want to limit the number of active jobs in a category use the following
    # limit_jobs_for_category("scavenge", 1)

    # Generate the Pegasus DAX file & a Submit Script
    write_workflow_description(tmp_path)


if __name__ == "__main__":
    parameters_only_entry_point(example_workflow)
