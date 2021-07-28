from pathlib import Path
from random import Random

from immutablecollections import immutableset
from vistautils.parameters import Parameters

from pegasus_wrapper import (
    PegasusProfile,
    add_container,
    directory_for,
    experiment_directory,
    initialize_vista_pegasus_wrapper,
    limit_jobs_for_category,
    run_bash,
    run_python_on_args,
    run_python_on_parameters,
    start_docker_as_service,
    stop_docker_as_service,
    write_workflow_description,
)
from pegasus_wrapper.artifact import ValueArtifact
from pegasus_wrapper.locator import Locator, _parse_parts
from pegasus_wrapper.pegasus_utils import build_submit_script
from pegasus_wrapper.resource_request import SlurmResourceRequest
from pegasus_wrapper.scripts.add_y import main as add_main
from pegasus_wrapper.scripts.multiply_by_x import main as multiply_by_x_main
from pegasus_wrapper.scripts.sort_nums_in_file import main as sort_nums_main

import pytest
from yaml import SafeLoader, load


def test_dax_with_job_on_saga(tmp_path):
    workflow_params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_directory": str(tmp_path / "working"),
            "site": "saga",
            "namespace": "test",
            "partition": "gaia",
            "experiment_name": "fred",
            "home_dir": str(tmp_path),
        }
    )
    slurm_params = Parameters.from_mapping(
        {"partition": "gaia", "num_cpus": 1, "num_gpus": 0, "memory": "4G"}
    )
    multiply_input_file = tmp_path / "raw_nums.txt"
    random = Random()
    random.seed(0)
    nums = immutableset(int(random.random() * 100) for _ in range(25))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    sorted_output_file = tmp_path / "sorted_nums.txt"
    add_output_file = tmp_path / "add_nums.txt"
    with multiply_input_file.open("w") as mult_file:
        mult_file.writelines(f"{num}\n" for num in nums)
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )
    sort_params = Parameters.from_mapping(
        {"input_file": multiply_output_file, "output_file": sorted_output_file}
    )
    add_args = f"{sorted_output_file} {add_output_file} --y 10"

    resources = SlurmResourceRequest.from_parameters(slurm_params)
    initialize_vista_pegasus_wrapper(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=run_python_on_parameters(
            multiply_job_name, multiply_by_x_main, multiply_params, depends_on=[]
        ),
        locator=Locator("multiply"),
    )
    multiple_dir = directory_for(multiply_job_name)
    assert (multiple_dir / "___run.sh").exists()
    assert (multiple_dir / "____params.params").exists()

    sort_job_name = Locator(_parse_parts("jobs/sort"))
    sort_dir = directory_for(sort_job_name)
    sort_artifact = run_python_on_parameters(
        sort_job_name,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=resources,
        category="add",
    )
    assert (sort_dir / "___run.sh").exists()
    assert (sort_dir / "____params.params").exists()

    add_job_name = Locator(_parse_parts("jobs/add"))
    add_dir = directory_for(add_job_name)
    run_python_on_args(add_job_name, add_main, add_args, depends_on=[sort_artifact])
    assert (add_dir / "___run.sh").exists()

    dax_file_one = write_workflow_description(tmp_path)
    dax_file_two = write_workflow_description()

    assert dax_file_one.exists()
    assert dax_file_two.exists()

    submit_script_one = tmp_path / "submit_script_one.sh"
    submit_script_two = tmp_path / "submit_script_two.sh"
    build_submit_script(submit_script_one, str(dax_file_one), experiment_directory())
    build_submit_script(submit_script_two, str(dax_file_two), experiment_directory())

    assert submit_script_one.exists()
    assert submit_script_two.exists()

    site_catalog = workflow_params.existing_directory("workflow_directory") / "sites.yml"
    assert site_catalog.exists()

    replica_catalog = (
        workflow_params.existing_directory("workflow_directory") / "replicas.yml"
    )
    assert replica_catalog.exists()

    transformations_catalog = (
        workflow_params.existing_directory("workflow_directory") / "transformations.yml"
    )
    assert transformations_catalog.exists()

    properties_file = (
        workflow_params.existing_directory("workflow_directory") / "pegasus.properties"
    )
    assert properties_file.exists()


def test_dax_with_checkpointed_jobs_on_saga(tmp_path):
    workflow_params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_directory": str(tmp_path / "working"),
            "site": "saga",
            "namespace": "test",
            "partition": "gaia",
            "home_dir": str(tmp_path),
        }
    )
    slurm_params = Parameters.from_mapping(
        {"partition": "gaia", "num_cpus": 1, "num_gpus": 0, "memory": "4G"}
    )
    resources = SlurmResourceRequest.from_parameters(slurm_params)
    initialize_vista_pegasus_wrapper(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )

    multiple_dir = directory_for(multiply_job_name)

    # Create checkpointed file so that when trying to create the job again,
    # Pegasus just adds the file to the Replica Catalog
    checkpointed_multiply_file = multiple_dir / "___ckpt"
    checkpointed_multiply_file.touch()
    multiply_output_file.touch()

    assert checkpointed_multiply_file.exists()
    assert multiply_output_file.exists()

    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=run_python_on_parameters(
            multiply_job_name, multiply_by_x_main, multiply_params, depends_on=[]
        ),
        locator=Locator("multiply"),
    )

    sort_job_name = Locator(_parse_parts("jobs/sort"))
    sorted_output_file = tmp_path / "sorted_nums.txt"
    sort_params = Parameters.from_mapping(
        {"input_file": multiply_output_file, "output_file": sorted_output_file}
    )
    run_python_on_parameters(
        sort_job_name,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=resources,
    )

    write_workflow_description()

    site_catalog = workflow_params.existing_directory("workflow_directory") / "sites.yml"
    assert site_catalog.exists()

    replica_catalog = (
        workflow_params.existing_directory("workflow_directory") / "replicas.yml"
    )
    assert replica_catalog.exists()

    transformations_catalog = (
        workflow_params.existing_directory("workflow_directory") / "transformations.yml"
    )
    assert transformations_catalog.exists()

    properties_file = (
        workflow_params.existing_directory("workflow_directory") / "pegasus.properties"
    )
    assert properties_file.exists()

    # Make sure the Replica Catalog is not empty
    assert replica_catalog.stat().st_size > 0


def test_clearing_ckpts(monkeypatch, tmp_path):

    workflow_params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_directory": str(tmp_path / "working"),
            "site": "saga",
            "namespace": "test",
            "partition": "scavenge",
            "home_dir": str(tmp_path),
        }
    )

    initialize_vista_pegasus_wrapper(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )

    multiple_dir = directory_for(multiply_job_name)

    checkpointed_multiply_file = multiple_dir / "___ckpt"
    checkpointed_multiply_file.touch()
    multiply_output_file.touch()

    run_python_on_parameters(
        multiply_job_name, multiply_by_x_main, multiply_params, depends_on=[]
    )
    monkeypatch.setattr("builtins.input", lambda _: "y")
    write_workflow_description()
    assert not checkpointed_multiply_file.exists()


def test_not_clearing_ckpts(monkeypatch, tmp_path):

    workflow_params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_directory": str(tmp_path / "working"),
            "site": "saga",
            "namespace": "test",
            "partition": "scavenge",
            "home_dir": str(tmp_path),
        }
    )

    initialize_vista_pegasus_wrapper(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )

    multiple_dir = directory_for(multiply_job_name)

    checkpointed_multiply_file = multiple_dir / "___ckpt"
    checkpointed_multiply_file.touch()
    multiply_output_file.touch()

    run_python_on_parameters(
        multiply_job_name, multiply_by_x_main, multiply_params, depends_on=[]
    )
    monkeypatch.setattr("builtins.input", lambda _: "n")
    write_workflow_description()
    assert checkpointed_multiply_file.exists()


def _job_in_dax_has_category(dax_file, target_job_locator, category):
    """
    Return whether the given DAX file has a job
    corresponding to `target_job_locator`
    which has the category `category`.
    """
    target_job_name = str(target_job_locator).replace("/", "_")

    with dax_file.open("r") as f:
        data = load(f, Loader=SafeLoader)

    for item in data["jobs"]:
        if item["name"] == target_job_name:
            if "dagman" in item["profiles"].keys():
                if "CATEGORY" in item["profiles"]["dagman"].keys():
                    if item["profiles"]["dagman"]["CATEGORY"] == category:
                        return True

    return False


def test_dax_with_categories(tmp_path):
    workflow_params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_directory": str(tmp_path / "working"),
            "site": "saga",
            "namespace": "test",
            "partition": "gaia",
            "home_dir": str(tmp_path),
        }
    )
    initialize_vista_pegasus_wrapper(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )
    multiply_job_category = "arithmetic"

    run_python_on_parameters(
        multiply_job_name,
        multiply_by_x_main,
        multiply_params,
        depends_on=[],
        category=multiply_job_category,
    )

    # Check that the multiply job has the appropriate category set in the DAX file
    dax_file = write_workflow_description()
    assert dax_file.exists()

    assert _job_in_dax_has_category(dax_file, multiply_job_name, multiply_job_category)
    assert not _job_in_dax_has_category(
        dax_file, multiply_job_name, "an-arbitrary-category"
    )


def test_dax_with_saga_categories(tmp_path):
    workflow_params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_directory": str(tmp_path / "working"),
            "site": "saga",
            "namespace": "test",
            "partition": "gaia",
            "home_dir": str(tmp_path),
        }
    )
    multiply_partition = "gaia"
    multiply_slurm_params = Parameters.from_mapping(
        {"partition": multiply_partition, "num_cpus": 1, "num_gpus": 0, "memory": "4G"}
    )
    multiply_resources = SlurmResourceRequest.from_parameters(multiply_slurm_params)
    initialize_vista_pegasus_wrapper(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )

    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=run_python_on_parameters(
            multiply_job_name,
            multiply_by_x_main,
            multiply_params,
            depends_on=[],
            resource_request=multiply_resources,
        ),
        locator=Locator("multiply"),
    )

    sort_partition = "lestat"
    sort_slurm_params = Parameters.from_mapping(
        {"partition": sort_partition, "num_cpus": 1, "num_gpus": 0, "memory": "4G"}
    )
    sort_resources = SlurmResourceRequest.from_parameters(sort_slurm_params)

    sort_job_name = Locator(_parse_parts("jobs/sort"))
    sorted_output_file = tmp_path / "sorted_nums.txt"
    sort_params = Parameters.from_mapping(
        {"input_file": multiply_output_file, "output_file": sorted_output_file}
    )
    run_python_on_parameters(
        sort_job_name,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=sort_resources,
    )

    dax_file = write_workflow_description()
    assert dax_file.exists()

    # Check that the multiply and sort jobs have the appropriate partition-defined categories set in
    # the DAX file
    assert _job_in_dax_has_category(dax_file, multiply_job_name, multiply_partition)
    assert not _job_in_dax_has_category(dax_file, multiply_job_name, sort_partition)
    assert _job_in_dax_has_category(dax_file, sort_job_name, sort_partition)
    assert not _job_in_dax_has_category(dax_file, sort_job_name, multiply_partition)


def test_category_max_jobs(tmp_path):
    workflow_params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_directory": str(tmp_path / "working"),
            "site": "saga",
            "namespace": "test",
            "partition": "gaia",
            "home_dir": str(tmp_path),
        }
    )
    multiply_slurm_params = Parameters.from_mapping(
        {"partition": "gaia", "num_cpus": 1, "num_gpus": 0, "memory": "4G"}
    )
    multiply_resources = SlurmResourceRequest.from_parameters(multiply_slurm_params)
    initialize_vista_pegasus_wrapper(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )

    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=run_python_on_parameters(
            multiply_job_name,
            multiply_by_x_main,
            multiply_params,
            depends_on=[],
            resource_request=multiply_resources,
        ),
        locator=Locator("multiply"),
    )

    sort_slurm_params = Parameters.from_mapping(
        {
            "partition": "ephemeral",
            "num_cpus": 1,
            "num_gpus": 0,
            "memory": "4G",
            "job_time_in_minutes": 120,
        }
    )
    sort_resources = SlurmResourceRequest.from_parameters(sort_slurm_params)

    sort_job_name = Locator(_parse_parts("jobs/sort"))
    sorted_output_file = tmp_path / "sorted_nums.txt"
    sort_params = Parameters.from_mapping(
        {"input_file": multiply_output_file, "output_file": sorted_output_file}
    )
    run_python_on_parameters(
        sort_job_name,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=sort_resources,
    )

    limit_jobs_for_category("gaia", 1)
    write_workflow_description()

    site_catalog = workflow_params.existing_directory("workflow_directory") / "sites.yml"
    assert site_catalog.exists()

    replica_catalog = (
        workflow_params.existing_directory("workflow_directory") / "replicas.yml"
    )
    assert replica_catalog.exists()

    transformations_catalog = (
        workflow_params.existing_directory("workflow_directory") / "transformations.yml"
    )
    assert transformations_catalog.exists()

    properties_file = (
        workflow_params.existing_directory("workflow_directory") / "pegasus.properties"
    )
    assert properties_file.exists()

    # Make sure the config contains the appropriate maxjobs lines and no inappropriate maxjobs lines
    with properties_file.open("r") as f:
        lines = f.readlines()
    for line in lines:
        print(line)
    assert any("dagman.gaia.maxjobs = 1" in line for line in lines)
    assert all("dagman.ephemeral.maxjobs =" not in line for line in lines)


def test_dax_test_exclude_nodes_on_saga(tmp_path):

    sample_exclude = "saga01,saga03,saga21,saga05"
    sample_include = "saga06"

    params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_directory": str(tmp_path / "working"),
            "site": "saga",
            "namespace": "test",
            "partition": "gaia",
            "exclude_list": sample_exclude,
            "home_dir": str(tmp_path),
        }
    )
    slurm_params = Parameters.from_mapping(
        {"partition": "gaia", "num_cpus": 1, "num_gpus": 0, "memory": "4G"}
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

    resources = SlurmResourceRequest.from_parameters(
        slurm_params.unify({"run_on_single_node": sample_include})
    )
    initialize_vista_pegasus_wrapper(params)

    multiply_job_locator = Locator(_parse_parts("jobs/multiply"))
    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=run_python_on_parameters(
            multiply_job_locator, multiply_by_x_main, multiply_params, depends_on=[]
        ),
        locator=Locator("multiply"),
    )
    sort_job_locator = Locator(_parse_parts("jobs/sort"))
    run_python_on_parameters(
        sort_job_locator,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=resources,
    )

    dax_file = write_workflow_description(tmp_path)
    with dax_file.open("r") as dax:
        dax_yaml = load(dax, Loader=SafeLoader)
    root = dax_yaml["jobs"]

    for item in root:
        if item["type"] == "job":
            if "pegasus" in item["profiles"]:
                if item["name"] == "jobs_multiply":
                    assert (
                        f"--exclude={sample_exclude}"
                        in item["profiles"]["pegasus"]["glite.arguments"]
                    )
                elif item["name"] == "jobs_sort":
                    assert "--exclude=" in item["profiles"]["pegasus"]["glite.arguments"]
                    assert (
                        f"--nodelist={sample_include}"
                        in item["profiles"]["pegasus"]["glite.arguments"]
                    )
                else:
                    assert False


def test_dax_with_job_in_container(tmp_path):
    workflow_params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_directory": str(tmp_path / "working"),
            "site": "saga",
            "namespace": "test",
            "partition": "gaia",
            "experiment_name": "fred",
            "home_dir": str(tmp_path),
        }
    )

    slurm_params = Parameters.from_mapping(
        {"partition": "gaia", "num_cpus": 1, "num_gpus": 0, "memory": "4G"}
    )

    multiply_input_file = tmp_path / "raw_nums.txt"
    random = Random()
    random.seed(0)
    nums = immutableset(int(random.random() * 100) for _ in range(25))
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
    initialize_vista_pegasus_wrapper(workflow_params)

    # Add Container
    example_docker = add_container("example_container", "docker", tmp_path / "docker.img")

    with pytest.raises(ValueError):
        _ = add_container("fake_container", "invalid", tmp_path / "invalid_docker.img")

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))

    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=run_python_on_parameters(
            multiply_job_name,
            multiply_by_x_main,
            multiply_params,
            depends_on=[],
            container=example_docker,
        ),
        locator=Locator("multiply"),
    )
    multiple_dir = directory_for(multiply_job_name)
    assert (multiple_dir / "___run.sh").exists()
    assert (multiple_dir / "____params.params").exists()

    sort_job_name = Locator(_parse_parts("jobs/sort"))
    sort_dir = directory_for(sort_job_name)
    run_python_on_parameters(
        sort_job_name,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=resources,
        container=example_docker,
    )
    assert (sort_dir / "___run.sh").exists()
    assert (sort_dir / "____params.params").exists()

    dax_file_one = write_workflow_description()

    assert dax_file_one.exists()

    site_catalog = workflow_params.existing_directory("workflow_directory") / "sites.yml"
    assert site_catalog.exists()

    replica_catalog = (
        workflow_params.existing_directory("workflow_directory") / "replicas.yml"
    )
    assert replica_catalog.exists()

    transformations_catalog = (
        workflow_params.existing_directory("workflow_directory") / "transformations.yml"
    )
    assert transformations_catalog.exists()

    properties_file = (
        workflow_params.existing_directory("workflow_directory") / "pegasus.properties"
    )
    assert properties_file.exists()


def test_dax_with_job_on_saga_with_dict_as_params(tmp_path):
    workflow_params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_directory": str(tmp_path / "working"),
            "site": "saga",
            "namespace": "test",
            "partition": "gaia",
            "experiment_name": "fred",
            "home_dir": str(tmp_path),
        }
    )
    slurm_params = Parameters.from_mapping(
        {"partition": "gaia", "num_cpus": 1, "num_gpus": 0, "memory": "4G"}
    )
    multiply_input_file = tmp_path / "raw_nums.txt"
    random = Random()
    random.seed(0)
    nums = immutableset(int(random.random() * 100) for _ in range(25))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    sorted_output_file = tmp_path / "sorted_nums.txt"
    add_output_file = tmp_path / "add_nums.txt"
    with multiply_input_file.open("w") as mult_file:
        mult_file.writelines(f"{num}\n" for num in nums)
    multiply_params = {
        "input_file": multiply_input_file,
        "output_file": multiply_output_file,
        "x": 4,
    }

    sort_params = {"input_file": multiply_output_file, "output_file": sorted_output_file}

    add_args = f"{sorted_output_file} {add_output_file} --y 10"

    job_profile = PegasusProfile(
        namespace="pegasus", key="transfer.bypass.input.staging", value="True"
    )
    resources = SlurmResourceRequest.from_parameters(slurm_params)
    initialize_vista_pegasus_wrapper(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=run_python_on_parameters(
            multiply_job_name,
            multiply_by_x_main,
            multiply_params,
            depends_on=[],
            job_profiles=[job_profile],
        ),
        locator=Locator("multiply"),
    )
    multiple_dir = directory_for(multiply_job_name)
    assert (multiple_dir / "___run.sh").exists()
    assert (multiple_dir / "____params.params").exists()

    sort_job_name = Locator(_parse_parts("jobs/sort"))
    sort_dir = directory_for(sort_job_name)
    sort_artifact = run_python_on_parameters(
        sort_job_name,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=resources,
        category="add",
    )
    assert (sort_dir / "___run.sh").exists()
    assert (sort_dir / "____params.params").exists()

    add_job_name = Locator(_parse_parts("jobs/add"))
    add_dir = directory_for(add_job_name)
    run_python_on_args(
        add_job_name, "add_job_main.py", add_args, depends_on=[sort_artifact]
    )
    assert (add_dir / "___run.sh").exists()

    dax_file_one = write_workflow_description(tmp_path)
    dax_file_two = write_workflow_description()

    assert dax_file_one.exists()
    assert dax_file_two.exists()

    submit_script_one = tmp_path / "submit_script_one.sh"
    submit_script_two = tmp_path / "submit_script_two.sh"
    build_submit_script(submit_script_one, str(dax_file_one), experiment_directory())
    build_submit_script(submit_script_two, str(dax_file_two), experiment_directory())

    assert submit_script_one.exists()
    assert submit_script_two.exists()

    site_catalog = workflow_params.existing_directory("workflow_directory") / "sites.yml"
    assert site_catalog.exists()

    replica_catalog = (
        workflow_params.existing_directory("workflow_directory") / "replicas.yml"
    )
    assert replica_catalog.exists()

    transformations_catalog = (
        workflow_params.existing_directory("workflow_directory") / "transformations.yml"
    )
    assert transformations_catalog.exists()

    properties_file = (
        workflow_params.existing_directory("workflow_directory") / "pegasus.properties"
    )
    assert properties_file.exists()


def test_dax_with_python_into_container_jobs(tmp_path):
    docker_tar = Path(f"{tmp_path}/docker/tar.tar")
    docker_build_dir = tmp_path
    docker_image_name = "pegasus_wrapper_container_demo"
    docker_image_tag = "0.2"

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

    # Write a list of numbers out to be able to run the workflow
    with input_file.open("w") as mult_file:
        mult_file.writelines(f"{num}\n" for num in nums)

    initialize_vista_pegasus_wrapper(workflow_params)

    build_container_locator = job_locator / "build_docker"
    build_container = run_bash(
        build_container_locator,
        command=[
            "mkdir -p /scratch/dockermount/pegasus_wrapper_tmp",
            f"cd {docker_build_dir}",
            f"docker build . -t {docker_image_name}:{docker_image_tag}",
            f"docker save -o /scratch/dockermount/pegasus_wrapper_tmp/{docker_tar.name} {docker_image_name}:{docker_image_tag}",
            f"cp /scratch/dockermount/pegasus_wrapper_tmp/{docker_tar.name} {docker_tar.absolute()}",
            f"chmod go+r {docker_tar.absolute()}",
        ],
        depends_on=[],
        resource_request=saga31_request,
    )
    build_container_dir = directory_for(build_container_locator)
    assert (build_container_dir / "script.sh").exists()

    python36 = add_container(
        f"{docker_image_name}:{docker_image_tag}",
        "docker",
        str(docker_tar.absolute()),
        image_site="saga",
        bypass_staging=True,
    )

    job_profile = PegasusProfile(
        namespace="pegasus", key="transfer.bypass.input.staging", value="True"
    )

    mongo4_4 = add_container(
        "mongo:4.4", "docker", "path/to/tar.tar", image_site="saga", bypass_staging=True
    )

    with pytest.raises(RuntimeError):
        _ = stop_docker_as_service(
            mongo4_4, depends_on=[], resource_request=saga31_request
        )

    start_mongo = start_docker_as_service(
        mongo4_4,
        depends_on=[build_container],
        docker_args=f"-v /scratch/mongo/data/db:/data/db",
        resource_request=saga31_request,
    )
    mongo4_4_dir = directory_for(Locator(("containers", mongo4_4.name)))
    assert (mongo4_4_dir / "start.sh").exists()
    assert (mongo4_4_dir / "stop.sh").exists()

    add_y_locator = job_locator / "add"
    add_y_job = run_python_on_args(
        add_y_locator,
        docker_python_root / "add_y.py",
        set_args=f"{input_file} {add_y_output_file_nas} --y 10",
        depends_on=[build_container],
        job_profiles=[job_profile],
        resource_request=saga31_request,
        container=python36,
        input_file_paths=[input_file],
        output_file_paths=[add_y_output_file_nas],
    )
    add_y_dir = directory_for(add_y_locator)
    assert (add_y_dir / "___run.sh").exists()

    with pytest.raises(RuntimeError):
        _ = run_python_on_args(
            add_y_locator,
            docker_python_root / "add_y.py",
            set_args=f"{input_file} {add_y_output_file_nas} --y 10",
            depends_on=[build_container],
            job_profiles=[job_profile],
            resource_request=saga31_request,
            container=python36,
            input_file_paths=[input_file, input_file],
            output_file_paths=[add_y_output_file_nas],
        )

    sort_job_locator = job_locator / "sort"
    sort_job = run_python_on_parameters(
        sort_job_locator,
        sort_nums_main,
        {"input_file": add_y_output_file_nas, "output_file": sorted_output_file_nas},
        depends_on=[add_y_job],
        container=python36,
        job_profiles=[job_profile],
        resource_request=saga31_request,
        input_file_paths=add_y_output_file_nas,
        output_file_paths=sorted_output_file_nas,
    )
    assert sort_job == run_python_on_parameters(
        sort_job_locator,
        sort_nums_main,
        {"input_file": add_y_output_file_nas, "output_file": sorted_output_file_nas},
        depends_on=[add_y_job],
        container=python36,
        job_profiles=[job_profile],
        resource_request=saga31_request,
        input_file_paths=add_y_output_file_nas,
        output_file_paths=sorted_output_file_nas,
    )
    sort_job_dir = directory_for(sort_job_locator)
    assert (sort_job_dir / "___run.sh").exists()
    assert (sort_job_dir / "____params.params").exists()

    with pytest.raises(RuntimeError):
        _ = run_python_on_parameters(
            sort_job_locator,
            sort_nums_main,
            {"input_file": add_y_output_file_nas, "output_file": sorted_output_file_nas},
            depends_on=[add_y_job],
            container=python36,
            job_profiles=[job_profile],
            resource_request=saga31_request,
            input_file_paths=add_y_output_file_nas,
            output_file_paths=[sorted_output_file_nas, sorted_output_file_nas],
        )

    celebration_bash_locator = job_locator / "celebrate"
    celebration_bash = run_bash(
        celebration_bash_locator,
        'echo "Jobs Runs Successfully"',
        depends_on=[sort_job],
        job_profiles=[job_profile],
    )
    assert celebration_bash == run_bash(
        celebration_bash_locator,
        'echo "Jobs Runs Successfully"',
        depends_on=[sort_job],
        job_profiles=[job_profile],
    )
    celebration_bash_dir = directory_for(celebration_bash_locator)
    assert (celebration_bash_dir / "script.sh").exists()

    _ = stop_docker_as_service(
        mongo4_4, depends_on=[start_mongo, sort_job], resource_request=saga31_request
    )

    # Generate the Pegasus DAX file & a Submit Script
    dax_file_one = write_workflow_description(tmp_path)
    assert dax_file_one.exists()

    submit_script_one = tmp_path / "submit.sh"
    assert submit_script_one.exists()
