from typing import Iterable, TypeVar, Union

T = TypeVar("T")


def _ensure_iterable(x: Union[T, Iterable[T]]) -> Iterable[T]:
    if isinstance(x, Iterable) and not isinstance(x, str):
        return x
    else:
        return (x,)
