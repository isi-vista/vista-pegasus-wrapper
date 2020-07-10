from pathlib import Path
from typing import Optional

from Pegasus.api import Arch
from Pegasus.DAX3 import OS, PFN, Executable, File

SUBMIT_SCRIPT = """#!/bin/bash

set -e

pegasus-plan \\
    --conf pegasus.conf \\
    --dax {dax_file} \\
    --dir {workflow_directory} \\
    --cleanup leaf \\
    --force \\
    --sites saga \\
    --output-site local \\
    --submit
"""


def path_to_pegasus_file(
    path: Path,
    *,
    site: str = "local",
    name: Optional[str] = None,
    is_raw_input: bool = False
) -> File:
    """
    Given a *path* object return a pegasus `File` for usage in a workflow
    If the resource is not on a local machine provide the *site* string.
    Files can be used for either an input or output of a Job.

    Args:
        path: path to the file
        site: site to be used, default is local. Should be set to saga if running
        on cluster.
        name: name given to the file
        is_raw_input: indicates that the file doesn't come from the output of another
        job in the workflow, so can be safely added to the Pegasus DAX
    Returns:
        Pegasus File at the given path

    """
    rtnr = File(name if name else str(path.absolute()).replace("/", "-"))
    if is_raw_input:
        rtnr.addPFN(path_to_pfn(path, site=site))
    return rtnr


def path_to_pfn(path: Path, *, site: str = "local") -> PFN:
    return PFN(str(path.absolute()), site=site)


def script_to_pegasus_executable(
    path: Path,
    name: Optional[str] = None,
    *,
    site: str = "local",
    namespace: Optional[str] = None,
    version: Optional[str] = None,
    arch: Optional[Arch] = None,
    os: Optional[OS] = None,
    osrelease: Optional[str] = None,
    osversion: Optional[str] = None,
    glibc: Optional[str] = None,
    installed: Optional[bool] = None,
    container: Optional[str] = None
) -> Executable:
    """
    Turns a script path into a pegasus Executable

    Arguments:
        *name*: Logical name of executable
        *namespace*: Executable namespace
        *version*: Executable version
        *arch*: Architecture that this exe was compiled for
        *os*: Name of os that this exe was compiled for
        *osrelease*: Release of os that this exe was compiled for
        *osversion*: Version of os that this exe was compiled for
        *glibc*: Version of glibc this exe was compiled against
        *installed*: Is the executable installed (true), or stageable (false)
        *container*: Optional attribute to specify the container to use
    """

    rtrnr = Executable(
        path.stem + path.suffix if name is None else name,
        namespace=namespace,
        version=version,
        arch=arch,
        os=os,
        osrelease=osrelease,
        osversion=osversion,
        glibc=glibc,
        installed=installed,
        container=container,
    )
    rtrnr.addPFN(path_to_pfn(path, site=site))
    return rtrnr


def build_submit_script(path: Path, dax_file: str, workflow_directory: Path) -> None:
    path.write_text(
        SUBMIT_SCRIPT.format(workflow_directory=workflow_directory, dax_file=dax_file)
    )
    # Designate the submit script as executable
    path.chmod(0o777)
