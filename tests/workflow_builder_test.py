import subprocess
from random import Random

from immutablecollections import immutableset
from vistautils.parameters import Parameters

from pegasus_wrapper.artifact import ValueArtifact
from pegasus_wrapper.locator import Locator, _parse_parts
from pegasus_wrapper.pegasus_utils import build_submit_script
from pegasus_wrapper.resource_request import SlurmResourceRequest
from pegasus_wrapper.workflow import WorkflowBuilder
from scripts.multiply_by_x import main as multiply_by_x_main
from scripts.sort_nums_in_file import main as sort_nums_main

import pytest


def test_simple_dax(tmp_path):
    params = Parameters.from_mapping(
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
    workflow_builder = WorkflowBuilder.from_parameters(params)
    assert workflow_builder.name == "Test"
    assert workflow_builder.created_by == "Testing"
    assert (
        workflow_builder._workflow_directory  # pylint:disable=protected-access
        == tmp_path / "working"
    )
    assert workflow_builder._namespace == "test"  # pylint:disable=protected-access
    assert workflow_builder._default_site == "saga"  # pylint:disable=protected-access
    assert workflow_builder.default_resource_request  # pylint:disable=protected-access
    assert workflow_builder._job_graph is not None  # pylint:disable=protected-access


def test_locator():
    job = Locator(_parse_parts("job"))
    example = Locator(_parse_parts("example/path"))
    combined = example / job
    combined_from_string = example / "job"

    assert combined.__repr__() == combined_from_string.__repr__()
    with pytest.raises(RuntimeError):
        _ = combined / 90


def test_dax_with_job_on_saga(tmp_path):
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
    slurm_params = Parameters.from_mapping(
        {"partition": "scavenge", "num_cpus": 1, "num_gpus": 0, "memory": "4G"}
    )
    multiply_input_file = tmp_path / "raw_nums.txt"
    random = Random()
    random.seed(0)
    nums = immutableset(int(random.random() * 100) for _ in range(0, 25))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    sorted_output_file = tmp_path / "sorted_nums.txt"
    with multiply_input_file.open("w") as mult_file:
        mult_file.writelines(f"{num}\n" for num in nums)
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )
    sort_params = Parameters.from_mapping(
        {"input_file": multiply_output_file, "output_file": sorted_output_file}
    )

    resources = SlurmResourceRequest.from_parameters(slurm_params)
    workflow_builder = WorkflowBuilder.from_parameters(workflow_params)

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
    sort_dir = workflow_builder.directory_for(sort_job_name)
    workflow_builder.run_python_on_parameters(
        sort_job_name,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=resources,
    )
    assert (sort_dir / "___run.sh").exists()
    assert (sort_dir / "____params.params").exists()

    dax_file_one = workflow_builder.write_dax_to_dir(tmp_path)
    dax_file_two = workflow_builder.write_dax_to_dir()

    assert dax_file_one.exists()
    assert dax_file_two.exists()

    submit_script_one = tmp_path / "submit_script_one.sh"
    submit_script_two = tmp_path / "submit_script_two.sh"
    build_submit_script(
        submit_script_one,
        str(dax_file_one),
        workflow_builder._workflow_directory,  # pylint:disable=protected-access
    )
    build_submit_script(
        submit_script_two,
        str(dax_file_two),
        workflow_builder._workflow_directory,  # pylint:disable=protected-access
    )

    assert submit_script_one.exists()
    assert submit_script_two.exists()

    submit_script_process = subprocess.Popen(
        ["sh", str(submit_script_one)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )
    stdout, stderr = submit_script_process.communicate()

    print(stdout)
    print(stderr)


def test_dax_with_checkpointed_jobs_on_saga(tmp_path):
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
    slurm_params = Parameters.from_mapping(
        {"partition": "scavenge", "num_cpus": 1, "num_gpus": 0, "memory": "4G"}
    )
    resources = SlurmResourceRequest.from_parameters(slurm_params)
    workflow_builder = WorkflowBuilder.from_parameters(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )

    multiple_dir = workflow_builder.directory_for(multiply_job_name)

    # Create checkpointed file so that when trying to create the job again,
    # Pegasus just adds the file to the Replica Catalog
    checkpointed_multiply_file = multiple_dir / "___ckpt"
    checkpointed_multiply_file.touch()
    multiply_output_file.touch()

    assert checkpointed_multiply_file.exists()
    assert multiply_output_file.exists()

    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=workflow_builder.run_python_on_parameters(
            multiply_job_name, multiply_by_x_main, multiply_params, depends_on=[]
        ),
        locator=Locator("multiply"),
    )

    sort_job_name = Locator(_parse_parts("jobs/sort"))
    sorted_output_file = tmp_path / "sorted_nums.txt"
    sort_params = Parameters.from_mapping(
        {"input_file": multiply_output_file, "output_file": sorted_output_file}
    )
    workflow_builder.run_python_on_parameters(
        sort_job_name,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=resources,
    )

    replica_catalog = workflow_params.existing_directory("workflow_directory") / "rc.dat"
    assert replica_catalog.exists()

    # Make sure the Replica Catalog is not empty
    assert replica_catalog.stat().st_size > 0
