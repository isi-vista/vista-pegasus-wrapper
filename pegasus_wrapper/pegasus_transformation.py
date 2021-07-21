from typing import Optional

from attr import attrib, attrs
from attr.validators import instance_of, optional

from Pegasus.api import Container, Transformation


@attrs(slots=True, frozen=True, repr=False)
class PegasusTransformation:
    r"""
    Internal transformation class to store additional data for automatic ISI specific defaults
    """

    name: str = attrib(validator=instance_of(str))
    transformation: Transformation = attrib(validator=instance_of(Transformation))
    container: Optional[Container] = attrib(validator=optional(instance_of(Container)))
