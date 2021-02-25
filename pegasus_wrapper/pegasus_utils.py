from pathlib import Path

from vistautils.parameters import Parameters

from Pegasus.api import (
    OS,
    Arch,
    Directory,
    FileServer,
    Operation,
    Properties,
    Site,
    SiteCatalog,
)

SUBMIT_SCRIPT = """#!/bin/bash

set -e

pegasus-plan \\
    {dax_file} \\
    --conf pegasus.properties \\
    --dir {workflow_directory} \\
    --cleanup leaf \\
    --force \\
    --sites saga \\
    --output-site local \\
    --submit
"""


def build_submit_script(path: Path, dax_file: str, workflow_directory: Path) -> None:
    path.write_text(
        SUBMIT_SCRIPT.format(workflow_directory=workflow_directory, dax_file=dax_file)
    )
    # Designate the submit script as executable
    path.chmod(0o777)


def add_local_nas_to_sites(
    sites_catalog: SiteCatalog, params: Parameters = Parameters.empty()
) -> None:
    home = params.string("home_dir", default=str(Path.home().absolute()))
    shared_scratch_dir = params.string(
        "local_shared_scratch", default=f"{home}/workflows/scratch"
    )
    local_storage_dir = params.string("local_storage", default=f"{home}/workflows/output")

    sites_catalog.add_sites(
        Site("local", arch=Arch.X86_64, os_type=OS.LINUX).add_directories(
            Directory(Directory.SHARED_SCRATCH, shared_scratch_dir).add_file_servers(
                FileServer("file://" + shared_scratch_dir, Operation.ALL)
            ),
            Directory(Directory.LOCAL_STORAGE, local_storage_dir).add_file_servers(
                FileServer("file://" + local_storage_dir, Operation.ALL)
            ),
        )
    )


def add_saga_cluster_to_sites(
    sites_catalog: SiteCatalog, params: Parameters = Parameters.empty()
) -> None:
    home = params.string("home_dir", default=str(Path.home().absolute()))

    shared_scratch_dir = params.string(
        "saga_shared_scratch", default=f"{home}/workflows/shared-scratch"
    )

    saga = Site("saga", arch=Arch.X86_64, os_type=OS.LINUX)
    saga.add_directories(
        Directory(Directory.SHARED_SCRATCH, shared_scratch_dir).add_file_servers(
            FileServer("file://" + shared_scratch_dir, Operation.ALL)
        )
    )

    saga.add_env(
        key="PEGASUS_HOME", value="/nas/gaia/shared/cluster/pegasus5/pegasus-5.0.0"
    )

    # Profiles
    saga.add_pegasus_profile(
        style="glite", auxillary_local=True, data_configuration="sharedfs"
    )
    saga.add_condor_profile(grid_resource="batch slurm")

    sites_catalog.add_sites(saga)


def configure_saga_properities(  # pylint: disable=unused-argument
    properties: Properties, params: Parameters = Parameters.empty()
) -> None:
    properties["pegasus.data.configuration"] = "sharedfs"
    properties["pegasus.metrics.app"] = "SAGA"
    properties["dagman.retry"] = "0"

    # TODO: Implement a method to add parameters to this properties file
    # See: https://github.com/isi-vista/vista-pegasus-wrapper/issues/72
