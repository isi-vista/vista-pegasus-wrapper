from typing import Generic, Iterable, Optional, TypeVar, Union

from attr import attrib, attrs
from attr.validators import instance_of, optional

from immutablecollections import ImmutableSet, immutableset

from pegasus_wrapper.locator import Locator
from pegasus_wrapper.workflow import DependencyNode

from typing_extensions import Protocol


class Artifact(Protocol):
    r"""
    An `Artifact` is the result of any computation.

    We know what jobs (represented by `DependencyNode`\ s it was *compute_by*
    and it may have an optional associated `Locator`.

    You can make your own sub-classes of `Artifact`
    which provide additional information about a computation result
    (see `AbstractArtifact`).

    Note that because `Artifact`\ s are instantiated
    before any computation actually happens,
    all their fields must point to information known in advance.
    For example, you can't include the actual content of a computation,
    but you can include the path where you know the result will be written.
    """

    computed_by: ImmutableSet[DependencyNode]
    locator: Optional[Locator]


def _to_dependency_set(
    item: Union[DependencyNode, Iterable[DependencyNode]]
) -> ImmutableSet[DependencyNode]:
    if isinstance(item, DependencyNode):
        return immutableset([item])
    else:
        return immutableset(item)


@attrs(frozen=True)
class AbstractArtifact(Artifact):
    """
    A convenient base class for custom `Artifact` implementations.
    """

    computed_by: ImmutableSet[DependencyNode] = attrib(
        converter=_to_dependency_set, kw_only=True, default=immutableset()
    )
    locator: Optional[Locator] = attrib(
        validator=optional(instance_of(Locator)), kw_only=True
    )


_T = TypeVar("_T")


@attrs(frozen=True)
class ValueArtifact(AbstractArtifact, Generic[_T]):
    """
    An artifact which wraps a single value.
    """

    value: _T = attrib()

    @staticmethod
    def preexisting(
        value: _T, *, locator: Optional[Locator] = None
    ) -> "ValueArtifact[_T]":
        return ValueArtifact(value, computed_by=immutableset(), locator=locator)

    @staticmethod
    def computed(
        value: _T,
        *,
        computed_by: Union[DependencyNode, Iterable[DependencyNode]],
        locator: Optional[Locator] = None,
    ) -> "ValueArtifact[_T]":
        if isinstance(computed_by, DependencyNode):
            canonical_computed_by = immutableset([computed_by])
        else:
            canonical_computed_by = immutableset(computed_by)
        return ValueArtifact(value, computed_by=canonical_computed_by, locator=locator)
