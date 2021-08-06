from pathlib import Path

from attr import attrib, attrs
from attr.validators import instance_of


@attrs(frozen=True, eq=False, slots=True)
class PegasusContainerFile:
    name: str = attrib(validator=instance_of(str))
    nas: Path = attrib(validator=instance_of(Path))
    scratch: Path = attrib(validator=instance_of(Path))
    docker: Path = attrib(validator=instance_of(Path))
