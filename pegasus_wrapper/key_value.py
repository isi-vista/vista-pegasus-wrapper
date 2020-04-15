from pathlib import Path
from typing import Any, Iterable, Mapping, Tuple

from attr import attrib, attrs
from attr.validators import instance_of

from vistautils.parameters import Parameters
from vistautils.scripts import join_key_value_stores
from vistautils.scripts import split_key_value_store as split_entry_point

from pegasus_wrapper.artifact import AbstractArtifact, Artifact
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.workflow import WorkflowBuilder

from typing_extensions import Protocol


class KeyValueStore(Artifact, Protocol):
    def input_parameters(self) -> Mapping[str, Any]:
        """
        Parameters to be passed to an entry point to use this key-value store as input.
        """

    def output_parameters(self) -> Mapping[str, Any]:
        """
        Parameters to be passed to an entry point to use this key-value stores as output.
        """


@attrs(frozen=True)
class ZipKeyValueStore(AbstractArtifact, KeyValueStore):
    path: Path = attrib(validator=instance_of(Path))

    def input_parameters(self) -> Mapping[str, Any]:
        return {"type": "zip", "path": self.path}

    def output_parameters(self) -> Mapping[str, Any]:
        return {"type": "zip", "path": self.path}


class KeyValueTransform(Protocol):
    def __call__(
        self, input_zip: KeyValueStore, *, workflow_builder: WorkflowBuilder
    ) -> KeyValueStore:
        """
        A function which adds jobs to *workflow_builder* to transform *input_zip*
        into another `KeyValueZip`.
        """


def split_key_value_store(
    input_store: KeyValueStore, *, num_parts: int, workflow_builder: WorkflowBuilder
) -> Tuple[KeyValueStore]:
    if num_parts <= 0:
        raise RuntimeError("Number of parts must be positive")

    split_locator = input_store.locator / "split"
    split_output_dir = workflow_builder.directory_for(split_locator)
    split_job = workflow_builder.run_python_on_parameters(
        split_locator,
        split_entry_point,
        Parameters.from_mapping(
            {
                "input": input_store.input_parameters(),
                "num_slices": num_parts,
                "output_dir": split_output_dir,
            }
        ),
    )
    return tuple(
        ZipKeyValueStore(
            path=split_output_dir / f"{slice_index}.zip",
            computed_by=split_job,
            locator=split_locator / str(slice_index),
        )
        for slice_index in range(num_parts)
    )


def join_to_key_value_zip(
    key_value_zips_to_join: Iterable[ZipKeyValueStore],
    *,
    output_locator: Locator,
    workflow_builder: WorkflowBuilder,
) -> ZipKeyValueStore:
    output_zip_path = workflow_builder.directory_for(output_locator) / "joined.zip"
    join_job = workflow_builder.run_python_on_parameters(
        output_locator,
        join_key_value_stores,
        Parameters.from_mapping(
            {
                "input_store_list_file": [p.path for p in key_value_zips_to_join],
                "output": {"type": "zip", "path": output_zip_path},
            }
        ),
    )
    return ZipKeyValueStore(
        path=output_zip_path, locator=output_locator, computed_by=join_job
    )


def transform_key_value_store(
    input_store: KeyValueStore,
    transform: KeyValueTransform,
    *,
    output_locator: Locator,
    workflow_builder: WorkflowBuilder,
    parallelism: int,
) -> ZipKeyValueStore:
    return join_to_key_value_zip(
        [
            transform(split, workflow_builder=workflow_builder)
            for split in split_key_value_store(
                input_store, num_parts=parallelism, workflow_builder=workflow_builder
            )
        ],
        output_locator=output_locator,
        workflow_builder=workflow_builder,
    )
