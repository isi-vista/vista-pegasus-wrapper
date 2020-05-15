from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Tuple

from attr import attrib, attrs
from attr.validators import instance_of

from vistautils.parameters import Parameters
from vistautils.scripts import downsample_key_value_store, join_key_value_stores
from vistautils.scripts import split_key_value_store as split_entry_point

from pegasus_wrapper.artifact import AbstractArtifact, Artifact, ValueArtifact
from pegasus_wrapper.locator import Locator
from pegasus_wrapper.workflow import WorkflowBuilder

from typing_extensions import Protocol


class KeyValueStore(Artifact, Protocol):
    """
    A key-value store is any sort of a mapping between keys and values store in some manner.

    This represents a placeholder for a key-value store in a Pegasus workflow.
    See *key_value.py` in *vistautils* for the actual implementations of key-value stores.

    The most common implementation in our workflow
    is a zip file mapping document IDs to document content.

    Note that a key-value store may be empty (store no mappings).
    """

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
    """
    An Pegasus placeholder for a `KeyValueStore` backed by a zip file.
    """

    path: Path = attrib(validator=instance_of(Path))

    def input_parameters(self) -> Mapping[str, Any]:
        return {"type": "zip", "path": self.path}

    def output_parameters(self) -> Mapping[str, Any]:
        return {"type": "zip", "path": self.path}


class KeyValueTransform(Protocol):
    """
    A transformation of one `KeyValueStore` into another.
    """

    def __call__(
        self,
        input_zip: KeyValueStore,
        *,
        workflow_builder: WorkflowBuilder,
        output_locator: Optional[Locator] = None,
    ) -> KeyValueStore:
        """
        A function which adds jobs to *workflow_builder* to transform *input_zip*
        into another `KeyValueZip`.
        """


def split_key_value_store(
    input_store: KeyValueStore, *, num_parts: int, workflow_builder: WorkflowBuilder
) -> Tuple[KeyValueStore]:
    """
    Splits *input_store* into *num_parts* pieces of nearly equal size.

    Some of the resulting key-value stores may be empty.
    """
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
        depends_on=input_store,
    )
    return tuple(
        ZipKeyValueStore(
            path=split_output_dir / f"{slice_index}.zip",
            depends_on=split_job,
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
    key_value_zips_to_join = tuple(key_value_zips_to_join)
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
        depends_on=key_value_zips_to_join,
    )
    return ZipKeyValueStore(
        path=output_zip_path, locator=output_locator, depends_on=join_job
    )


def transform_key_value_store(
    input_store: KeyValueStore,
    transform: KeyValueTransform,
    *,
    output_locator: Locator,
    workflow_builder: WorkflowBuilder,
    parallelism: int,
) -> KeyValueStore:
    if parallelism > 1:
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
    elif parallelism == 1:
        # Not need for split/join overhead if there is no parallelism
        return transform(
            input_store, workflow_builder=workflow_builder, output_locator=output_locator
        )
    else:
        raise RuntimeError(f"Parallelism must be positive but got {parallelism}")


def downsample(
    input_store: KeyValueStore,
    *,
    limit: int,
    output_locator: Optional[Locator] = None,
    workflow_builder: WorkflowBuilder,
) -> KeyValueStore:
    """
    Convince function to run `vistautils.scripts.downsample_key_value_store` as a Pegasus Job
    """
    if not output_locator:
        output_locator = input_store.locator / f"downsampled-{limit}"
    output_zip_path = workflow_builder.directory_for(output_locator) / "downsampled.zip"
    downsample_job = workflow_builder.run_python_on_parameters(
        output_locator,
        downsample_key_value_store,
        Parameters.from_mapping(
            {
                "input": input_store.input_parameters(),
                "output_zip_path": output_zip_path,
                "num_to_sample": limit,
                "random_seed": 0,
            }
        ),
        depends_on=input_store,
    )
    return ZipKeyValueStore(
        path=output_zip_path,
        locator=output_locator,
        depends_on=[input_store.depends_on, downsample_job],
    )


@attrs(frozen=True, slots=True)
class DataSplit:
    r"""
    A train/dev/test split over `KeyValueStore`\ s.

    If any of these are missing, the corresponding `KeyValueStore` should
    refer to an empty key-value store.
    """
    train: KeyValueStore = attrib(validator=instance_of(KeyValueStore), kw_only=True)
    dev: KeyValueStore = attrib(validator=instance_of(KeyValueStore), kw_only=True)
    test: KeyValueStore = attrib(validator=instance_of(KeyValueStore), kw_only=True)


def explicit_train_dev_test_split(
    corpus: KeyValueStore,
    *,
    train_ids: ValueArtifact[Path],
    dev_ids: ValueArtifact[Path],
    test_ids: ValueArtifact[Path],
    output_locator: Locator,
    exhaustive: bool = True,
    downsample_to: Optional[int] = None,
    workflow_builder: WorkflowBuilder,
) -> DataSplit:
    """
    Explicit implementation for handling a train/dev/test split over a `KeyValueStore`

    The split is done by a list of keys handed explicitly to the user.

    If *exhaustive* is True then an exception will be thrown if a document does not get assigned to
    one of the three sets. This is to help prevent accidental omissions in the key lists

    *downsample_to* is an optional integer to reduce to the size of the key_value split
    for quicker debugging. See `vistautils.scripts.downsample_key_value_store` as the function
    which is

    See `DataSplit` for the output description.
    """
    train_locator = output_locator / "train"
    dev_locator = output_locator / "dev"
    test_locator = output_locator / "test"

    train_zip = workflow_builder.directory_for(train_locator) / "train.zip"
    dev_zip = workflow_builder.directory_for(dev_locator) / "dev.zip"
    test_zip = workflow_builder.directory_for(test_locator) / "test.zip"

    split_job = workflow_builder.run_python_on_parameters(
        output_locator,
        split_entry_point,
        parameters={
            "input": corpus.input_parameters(),
            "explicit_split": {
                "train": {"keys_file": train_ids.value, "output_file": train_zip},
                "dev": {"keys_file": dev_ids.value, "output_file": dev_zip},
                "test": {"keys_file": test_ids.value, "output_file": test_zip},
                "must_be_exhaustive": exhaustive,
            },
        },
        depends_on=[corpus],
    )

    deps = [
        corpus.depends_on,
        split_job,
        train_ids.depends_on,
        dev_ids.depends_on,
        test_ids.depends_on,
    ]

    train_store = ZipKeyValueStore(train_zip, locator=train_locator, depends_on=deps)
    dev_store = ZipKeyValueStore(dev_zip, locator=dev_locator, depends_on=deps)
    test_store = ZipKeyValueStore(test_zip, locator=test_locator, depends_on=deps)
    if downsample_to is None:
        return DataSplit(train=train_store, dev=dev_store, test=test_store)
    else:
        return DataSplit(
            train=downsample(
                train_store, limit=downsample_to, workflow_builder=workflow_builder
            ),
            dev=downsample(
                dev_store, limit=downsample_to, workflow_builder=workflow_builder
            ),
            test=downsample(
                test_store, limit=downsample_to, workflow_builder=workflow_builder
            ),
        )
