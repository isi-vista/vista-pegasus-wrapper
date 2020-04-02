from vistautils.parameters import Parameters

from pegasus_wrapper import WorkflowBuilder


def test_simple_dax(tmp_path):
    params = Parameters.from_mapping(
        {
            "workflow_name": "Test",
            "workflow_created": "Testing",
            "workflow_log_dir": str(tmp_path / "log"),
            "workflow_dir": str(tmp_path / "working/"),
        }
    )
    workflow_builder = WorkflowBuilder.from_params(params)
    assert workflow_builder.name == "Test"
    assert workflow_builder.created_by == "Testing"
