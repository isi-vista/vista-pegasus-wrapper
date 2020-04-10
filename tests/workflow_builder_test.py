from vistautils.parameters import Parameters

from pegasus_wrapper.workflow import WorkflowBuilder


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
    workflow_builder = WorkflowBuilder.from_params(params)
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
