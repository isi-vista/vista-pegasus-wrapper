from immutablecollections import immutableset
from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point

from pegasus_wrapper.locator import Locator, _parse_parts
from pegasus_wrapper.pegasus_utils import build_submit_script
from random import Random

from scripts.multiply_by_x import main as multiply_by_x_main
from scripts.sort_nums_in_file import main as sort_nums_main

from pegasus_wrapper.resource_request import SlurmResourceRequest
from pegasus_wrapper.workflow import WorkflowBuilder
from pegasus_wrapper.artifact import ValueArtifact


def example_workflow(params: Parameters):
    """
    Example Workflow Creator for individuals to create a simple DAX and submit
    to Pegasus for testing & learning purposes.
    """
    tmp_path = params.creatable_directory("dir")

    # Generating parameters for initializing a workflow
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
    nums = immutableset(int(random.random() * 100) for _ in range(0, 25))

    multiply_output_file = tmp_path / "multiplied_nums.txt"
    sorted_output_file = tmp_path / "sorted_nums.txt"

    # Write a list of numbers out to be able to run the workflow
    with multiply_input_file.open("w") as mult_file:
        mult_file.writelines(f"{num}\n" for num in nums)

    # Params for the multiply job
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "ouput_file": multiply_output_file, "x": 4}
    )
    # Params for the sort job
    sort_params = Parameters.from_mapping(
        {"input_file": multiply_output_file, "output_file": sorted_output_file}
    )

    resources = SlurmResourceRequest.from_parameters(slurm_params)
    workflow_builder = WorkflowBuilder.from_params(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=workflow_builder.run_python_on_parameters(
            multiply_job_name, multiply_by_x_main, multiply_params, depends_on=[]
        ),
        locator=Locator("multiply"),
    )
    multiple_dir = workflow_builder.directory_for(multiply_job_name)
    assert (multiple_dir / "___run.sh").exists()
    assert (multiple_dir / "____params.params").exists()

    sort_job_name = Locator(_parse_parts("jobs/sort"))
    workflow_builder.run_python_on_parameters(
        sort_job_name,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=resources,
    )

    # Generate the Pegasus DAX file
    dax_file = workflow_builder.write_dax_to_dir(tmp_path)

    submit_script = tmp_path / "submit_script.sh"

    # Our attempt at an easy submit file, it MAY NOT be accurate for more complicated workflows but it
    # does work for this simple example.
    # See https://github.com/isi-vista/vista-pegasus-wrapper/issues/27
    build_submit_script(
        submit_script,
        str(dax_file),
        workflow_builder._workflow_directory,  # pylint:disable=protected-access
    )


if __name__ == "__main__":
    parameters_only_entry_point(example_workflow)
