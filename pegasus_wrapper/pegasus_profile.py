from attr import attrib, attrs
from attr.validators import instance_of

from immutablecollections import immutabledict

from Pegasus.api import Namespace

_STR_TO_NAMESPACE = immutabledict(
    {
        "condor": Namespace.CONDOR,
        "dagman": Namespace.DAGMAN,
        "env": Namespace.ENV,
        "globus": Namespace.GLOBUS,
        "pegasus": Namespace.PEGASUS,
        "selector": Namespace.SELECTOR,
    }
)


def _get_namespace(str_namespace: str) -> Namespace:
    return _STR_TO_NAMESPACE[str_namespace]


@attrs(slots=True, frozen=True, repr=False)
class PegasusProfile:
    r"""
    A `PegasusProfile` is a profile that can be added to e.g. a Pegasus job.
    """

    namespace: Namespace = attrib(converter=_get_namespace)
    key: str = attrib(validator=instance_of(str))
    value: str = attrib(validator=instance_of(str))

    def __repr__(self) -> str:
        return f"({self.namespace}, key={self.key}, value={self.value})"
