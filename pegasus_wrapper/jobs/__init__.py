from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable, TypeVar

JobT = TypeVar("JobT", bound="PegasusJobBase")


class PegasusJobBase(ABC):
    job_name: str
    num_cpu: int
    num_gpu: int
    dependencies: Iterable[Path]
    outputs: Iterable[Path]

    @abstractmethod
    def to_pegasus_job(self) -> Any:
        """
        Create and return a pegasus job object
        """


class CondaJobBase(PegasusJobBase):
    conda_bin: Path
    conda_environment_name: str

    @abstractmethod
    def to_submit_script(self) -> Path:
        """
        Create a script which invokes a conda venv to run the requested job in a VM
        """

    @abstractmethod
    def to_pegasus_job(self) -> Any:
        """
        Create and return a pegasus job object
        """
