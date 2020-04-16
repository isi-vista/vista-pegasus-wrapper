from typing import Generic, Iterable, Optional, Sequence, TypeVar, Union

from attr import attrib, attrs
from attr.validators import instance_of, optional

from immutablecollections import ImmutableSet, immutableset

from Pegasus.DAX3 import Job
from pegasus_wrapper.locator import Locator

from more_itertools import collapse
from typing_extensions import Protocol


@attrs(slots=True, eq=False)
class DependencyNode:
    """
    An abstract object tied to a computation
    which can be used only for the purpose
    of indicating that one computation
    depends on the output of another computation.
    """

    job: Job = attrib(validator=instance_of(Job), kw_only=True)

    @staticmethod
    def from_job(job: Job) -> "DependencyNode":
        return DependencyNode(job=job)


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

    depends_on: ImmutableSet[DependencyNode]
    locator: Optional[Locator]


def _canonicalize_depends_on(
    dep_param, *, max_depth: Optional[int] = None
) -> Sequence[DependencyNode]:
    """
    For convenience, we allow specifying the depends_on parameter when submitting a job
    in numerous ways.
    """
    if max_depth is not None:
        if isinstance(dep_param, DependencyNode):
            return [dep_param]
        elif isinstance(dep_param, Artifact):
            return dep_param.depends_on
        elif max_depth > 0:
            return [
                _canonicalize_depends_on(x, max_depth=max_depth - 1) for x in dep_param
            ]
        else:
            raise RuntimeError("Error parsing dependency specification")
    else:
        return immutableset(collapse(_canonicalize_depends_on(dep_param, max_depth=2)))


@attrs(frozen=True)
class AbstractArtifact(Artifact):
    """
    A convenient base class for custom `Artifact` implementations.
    """

    depends_on: ImmutableSet[DependencyNode] = attrib(
        converter=_canonicalize_depends_on, kw_only=True, default=immutableset()
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
        return ValueArtifact(value, depends_on=immutableset(), locator=locator)

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
        return ValueArtifact(value, depends_on=canonical_computed_by, locator=locator)
