from vistautils.parameters import Parameters

from pegasus_wrapper import initialize_vista_pegasus_wrapper
from pegasus_wrapper.key_value import (
    compose_key_value_store_transforms,
    transform_key_value_store,
)
from pegasus_wrapper.locator import Locator


def test_composed_key_value_transform(tmp_path):
    kvs = {"doc1": 5, "doc2": 10}

    def add1(values, **kwargs):  # pylint:disable=unused-argument
        return {key: val + 1 for key, val in values.items()}

    def subtract2(values, **kwargs):  # pylint:disable=unused-argument
        return {key: val - 2 for key, val in values.items()}

    composed_transforms = compose_key_value_store_transforms(transforms=[add1, subtract2])

    params = Parameters.from_mapping(
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

    initialize_vista_pegasus_wrapper(params)

    transformed_kvs = transform_key_value_store(
        kvs, composed_transforms, output_locator=Locator([]), parallelism=1
    )

    expected_kvs = {"doc1": 4, "doc2": 9}
    assert expected_kvs == transformed_kvs
