from Pegasus.api.mixins import HookMixin
from attr import attrs
from vistautils.parameters import Parameters


@attrs(slots=True, eq=False)
class ShellTrigger:

    @staticmethod
    def from_params(params: Parameters) -> "ShellTrigger":
        raise NotImplementedError

    def apply_triggers(self, item: HookMixin):
        raise NotImplementedError
