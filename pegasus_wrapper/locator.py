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
    """
    fill me in
    """

    _parts: Tuple[str] = attrib(converter=_parse_parts)

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
