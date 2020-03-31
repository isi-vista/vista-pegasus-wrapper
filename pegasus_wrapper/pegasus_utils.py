from pathlib import Path
from typing import Optional

from Pegasus.DAX3 import PFN, File


def path_to_pegasus_file(
    path: Path, *, site: str = "local", name: Optional[str] = None
) -> File:
    """
    Given a *path* object return a pegasus `File` for usage in a workflow

    If the resource is not on a local machine provide the *site* string.

    Files can be used for either an input or output of a Job.
    """
    rtnr = File(path.stem + path.suffix if name is None else name)
    rtnr.addPFN(path_to_pfn(path, site=site))
    return rtnr


def path_to_pfn(path: Path, *, site: str = "local") -> PFN:
    return PFN(str(path.absolute()), site=site)