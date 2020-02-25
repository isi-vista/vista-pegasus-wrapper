from abc import ABC, abstractmethod
from pathlib import Path
from typing import Generic, Iterable

from pegasus_wrapper.jobs import JobT


class PegasusWorkflowGeneratorBase(ABC, Generic[JobT]):
    jobs: Iterable[JobT]

    @abstractmethod
    def to_pegasus_workflow(self, path_to_output: Path) -> None:
        """
        Generate a Pegasus DAX workflow
        """
