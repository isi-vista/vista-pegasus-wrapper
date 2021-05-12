import subprocess
from random import Random

from immutablecollections import immutableset
from vistautils.parameters import Parameters

from pegasus_wrapper.artifact import ValueArtifact
from pegasus_wrapper.locator import Locator, _parse_parts
from pegasus_wrapper.pegasus_utils import build_submit_script
from pegasus_wrapper.resource_request import SlurmResourceRequest
from pegasus_wrapper.scripts.add_y import main as add_main
from pegasus_wrapper.scripts.multiply_by_x import main as multiply_by_x_main
from pegasus_wrapper.scripts.sort_nums_in_file import main as sort_nums_main
from pegasus_wrapper.workflow import WorkflowBuilder

import pytest
from yaml import SafeLoader, load


def test_simple_dax(tmp_path):
    params = Parameters.from_mapping(
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
    assert workflow_builder._experiment_name == "fred"  # pylint:disable=protected-access


def test_locator():
    job = Locator(_parse_parts("job"))
    example = Locator(_parse_parts("example/path"))
    combined = example / job
    combined_from_string = example / "job"

    assert combined.__repr__() == combined_from_string.__repr__()
    with pytest.raises(RuntimeError):
        _ = combined / 90

    # Confirm we can't create a locator with an equals sign in the name
    with pytest.raises(ValueError):
        _ = Locator(_parse_parts("x=20"))


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
    sort_artifact = workflow_builder.run_python_on_parameters(
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
    add_dir = workflow_builder.directory_for(add_job_name)
    workflow_builder.run_python_on_args(
        add_job_name, add_main, add_args, depends_on=[sort_artifact]
    )
    assert (add_dir / "___run.sh").exists()

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
            "partition": "gaia",
            "home_dir": str(tmp_path),
        }
    )
    slurm_params = Parameters.from_mapping(
        {"partition": "gaia", "num_cpus": 1, "num_gpus": 0, "memory": "4G"}
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

    workflow_builder.write_dax_to_dir()

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

    workflow_builder = WorkflowBuilder.from_parameters(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )

    multiple_dir = workflow_builder.directory_for(multiply_job_name)

    checkpointed_multiply_file = multiple_dir / "___ckpt"
    checkpointed_multiply_file.touch()
    multiply_output_file.touch()

    workflow_builder.run_python_on_parameters(
        multiply_job_name, multiply_by_x_main, multiply_params, depends_on=[]
    )
    monkeypatch.setattr("builtins.input", lambda _: "y")
    workflow_builder.write_dax_to_dir()
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

    workflow_builder = WorkflowBuilder.from_parameters(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )

    multiple_dir = workflow_builder.directory_for(multiply_job_name)

    checkpointed_multiply_file = multiple_dir / "___ckpt"
    checkpointed_multiply_file.touch()
    multiply_output_file.touch()

    workflow_builder.run_python_on_parameters(
        multiply_job_name, multiply_by_x_main, multiply_params, depends_on=[]
    )
    monkeypatch.setattr("builtins.input", lambda _: "n")
    workflow_builder.write_dax_to_dir()
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
    workflow_builder = WorkflowBuilder.from_parameters(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )
    multiply_job_category = "arithmetic"

    workflow_builder.run_python_on_parameters(
        multiply_job_name,
        multiply_by_x_main,
        multiply_params,
        depends_on=[],
        category=multiply_job_category,
    )

    # Check that the multiply job has the appropriate category set in the DAX file
    dax_file = workflow_builder.write_dax_to_dir()
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
    workflow_builder = WorkflowBuilder.from_parameters(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )

    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=workflow_builder.run_python_on_parameters(
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
    workflow_builder.run_python_on_parameters(
        sort_job_name,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=sort_resources,
    )

    dax_file = workflow_builder.write_dax_to_dir()
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
    workflow_builder = WorkflowBuilder.from_parameters(workflow_params)

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))
    multiply_output_file = tmp_path / "multiplied_nums.txt"
    multiply_input_file = tmp_path / "raw_nums.txt"
    multiply_params = Parameters.from_mapping(
        {"input_file": multiply_input_file, "output_file": multiply_output_file, "x": 4}
    )

    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=workflow_builder.run_python_on_parameters(
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
    workflow_builder.run_python_on_parameters(
        sort_job_name,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=sort_resources,
    )

    workflow_builder.limit_jobs_for_category("gaia", 1)
    workflow_builder.write_dax_to_dir()

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
    workflow_builder = WorkflowBuilder.from_parameters(params)

    multiply_job_locator = Locator(_parse_parts("jobs/multiply"))
    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=workflow_builder.run_python_on_parameters(
            multiply_job_locator, multiply_by_x_main, multiply_params, depends_on=[]
        ),
        locator=Locator("multiply"),
    )
    sort_job_locator = Locator(_parse_parts("jobs/sort"))
    workflow_builder.run_python_on_parameters(
        sort_job_locator,
        sort_nums_main,
        sort_params,
        depends_on=[multiply_artifact],
        resource_request=resources,
    )

    dax_file = workflow_builder.write_dax_to_dir(tmp_path)
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
    workflow_builder = WorkflowBuilder.from_parameters(workflow_params)

    # Add Container
    example_docker = workflow_builder.add_container(
        "example_container", "docker", tmp_path / "docker.img"
    )

    with pytest.raises(ValueError):
        _ = workflow_builder.add_container(
            "fake_container", "invalid", tmp_path / "invalid_docker.img"
        )

    multiply_job_name = Locator(_parse_parts("jobs/multiply"))

    multiply_artifact = ValueArtifact(
        multiply_output_file,
        depends_on=workflow_builder.run_python_on_parameters(
            multiply_job_name,
            multiply_by_x_main,
            multiply_params,
            depends_on=[],
            container=example_docker,
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
        container=example_docker,
    )
    assert (sort_dir / "___run.sh").exists()
    assert (sort_dir / "____params.params").exists()

    dax_file_one = workflow_builder.write_dax_to_dir()

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
    sort_artifact = workflow_builder.run_python_on_parameters(
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
    add_dir = workflow_builder.directory_for(add_job_name)
    workflow_builder.run_python_on_args(
        add_job_name, "add_job_main.py", add_args, depends_on=[sort_artifact]
    )
    assert (add_dir / "___run.sh").exists()

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
