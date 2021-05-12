from itertools import chain
from typing import Iterable, Tuple, Union

from attr import attrib, attrs


def _parse_parts(string_or_sequence: Union[str, Iterable[str]]) -> Tuple[str]:
    if isinstance(string_or_sequence, str):
        return tuple(string_or_sequence.split("/"))
    else:
        return tuple(string_or_sequence)


@attrs(slots=True, frozen=True, repr=False)
class Locator:
    r"""
    A `Locator` is provides a structured name to a workflow task.

    These are used for assigning names to jobs in the underlying execution engine.

    It is also used to generate the location of job log files
    and often the working directories where jobs write their output.

    `Locator`\ s have a sequential/hierarchical structure similar to a `Path`.
    You can extend a `Locator` to create a new `Locator` using the */* operator,
    just like `Path`.
    """

    _parts: Tuple[str] = attrib(converter=_parse_parts)

    @_parts.validator
    def _validate_parts(self, _attr, parts):
        if any("=" in part for part in parts):
            # Pegasus uses HTCondor which can't handle = in job names, so we
            # forbid them to avoid confusion
            raise ValueError(f"Can't handle locator path containing =: `{parts}`.")

    def __truediv__(self, other: Union[str, "Locator"]):
        if isinstance(other, Locator):
            return Locator(
                chain(self._parts, other._parts)  # pylint:disable=protected-access
            )
        elif isinstance(other, str):
            new_parts = list(self._parts)
            new_parts.append(other)
            return Locator(new_parts)
        else:
            raise RuntimeError(f"Cannot extend a locator with a {type(other)}")

    def __repr__(self) -> str:
        return "/".join(self._parts)
