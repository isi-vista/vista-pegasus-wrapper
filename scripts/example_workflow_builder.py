from random import Random

from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point

from pegasus_wrapper import (
    experiment_directory,
    initialize_vista_pegasus_wrapper,
    run_python_on_parameters,
    write_dax_to_dir,
)
from pegasus_wrapper.artifact import ValueArtifact
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.pegasus_utils import build_submit_script
from pegasus_wrapper.resource_request import SlurmResourceRequest
from scripts import multiply_by_x, sort_nums_in_file


def example_workflow(params: Parameters):
    """
    An example script to generate a workflow for submission to Pegasus.
    """
    tmp_path = params.creatable_directory("dir")

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
            "partition": "scavenge",
        }
    )

    # Basic slurm resource request params
    slurm_params = Parameters.from_mapping(
        {"partition": "scavenge", "num_cpus": 1, "num_gpus": 0, "memory": "1G"}
    )

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

    resources = SlurmResourceRequest.from_parameters(slurm_params)
    initialize_vista_pegasus_wrapper(workflow_params)

    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=run_python_on_parameters(
            job_locator / "multiply",
            multiply_by_x,
            {
                "input_file": multiply_input_file,
                "ouput_file": multiply_output_file,
                "x": 4,
            },
            depends_on=[],
        ),
        locator=Locator("multiply"),
    )

    run_python_on_parameters(
        job_locator / "sort",
        sort_nums_in_file,
        {"input_file": multiply_output_file, "output_file": sorted_output_file},
        depends_on=[multiply_artifact],
        resource_request=resources,
    )

    # Generate the Pegasus DAX file
    dax_file = write_dax_to_dir(tmp_path)

    submit_script = tmp_path / "submit_script.sh"

    # Our attempt at an easy submit file, it MAY NOT be accurate for more complicated workflows but it
    # does work for this simple example.
    # See https://github.com/isi-vista/vista-pegasus-wrapper/issues/27
    build_submit_script(
        submit_script,
        str(dax_file),
        experiment_directory(),  # pylint:disable=protected-access
    )


if __name__ == "__main__":
    parameters_only_entry_point(example_workflow)
