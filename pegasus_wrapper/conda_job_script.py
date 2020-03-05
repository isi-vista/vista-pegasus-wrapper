import os
from pathlib import Path
from typing import Optional

from attr import attrs

from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point

from saga_tools.conda import CondaConfiguration
from saga_tools.spack import SpackConfiguration


def main(params: Parameters):
    runner = CondaJobScriptGenerator.from_parameters(params)
    entry_point = params.string("entry_point")
    runner.run_entry_point(
        entry_point_name=entry_point,
        param_file=params.existing_file("job_param_file"),
        conda_script_path=params.creatable_file("conda_script_path"),
        working_directory=params.optional_creatable_directory("working_directory")
        or Path(os.getcwd()),
        echo_template=params.boolean("echo_template", default=False),
    )


@attrs(frozen=True, slots=True)
class CondaJobScriptGenerator:
    """
    Driver to create a shell script in order to run a python program in a conda virtual environment

    The program takes a parameters file as input to generate the executable script. The script can
    be used as an executable job for Pegasus so that a job can be run in a venv as the default
    pegasus jobs do not run in one.

    The generated script is based on
    https://github.com/isi-vista/saga-tools/blob/master/saga_tools/slurm_run_python.py
    with the slurm related configuration removed.

    ####################
    Conda Job Parameters
    ####################
    * *conda_base_path*: path to the base of the conda install (not the *bin* directory)
    * *conda_environment*: name of the conda
        environment to run in
    * *spack_environment*: (optional): the spack environment, if any, to run in.
        Cannot appear with *spack_packages*.
    * *spack_packages* (optional): a YAML list of Spack packages to load(in *module@version* format)
    * *spack_root*: the spack installation to use (necessary only if *spack_environment*
        or *spack_modules*) is specified. This is usually the path to a working
        copy of a spack repository.
    * *entry_point*: the name of the module to run, e.g. vistautils.scripts.foo
    * *conda_script_path*: the file to write the conda job script to.
    * *job_param_file* (optional): the param file to call
        the job with
    * *job_name* (optional): The name of the job running via the script, defaults to entry_point
    * *working_directory* (optional)
    * *echo_template* (optional boolean, default False): whether to echo the generated
        job script (for debugging).
    """

    conda_config: Optional[CondaConfiguration]
    spack_config: Optional[SpackConfiguration]

    @staticmethod
    def from_parameters(params: Parameters) -> "CondaJobScriptGenerator":
        return CondaJobScriptGenerator(
            conda_config=CondaConfiguration.from_parameters(params),
            spack_config=SpackConfiguration.from_parameters(params),
        )

    def run_entry_point(
        self,
        entry_point_name,
        param_file: Path,
        conda_script_path: Path,
        *,
        working_directory: Path,
        echo_template: bool = False,
    ):

        conda_script_content = CONDA_SCRIPT.format(
            conda_lines=self.conda_config.sbatch_lines(),
            spack_lines=self.conda_config.sbatch_lines(),
            working_directory=working_directory,
            entry_point=entry_point_name,
            param_file=param_file,
        )

        conda_script_path.write_text(  # type: ignore
            conda_script_content, encoding="utf-8"
        )
        if echo_template:
            print(conda_script_content)


CONDA_SCRIPT = """#!/usr/bin/env bash
# This is needed because SLURM jobs are run from a non-interactive shell,
# but conda expects PS1 (the prompt variable) to be set.
if [[ -z ${{PS1+x}} ]]
  then
    export PS1=""
fi
{conda_lines}
{spack_lines}
cd {working_directory}
python -m {entry_point} {param_file}
"""

if __name__ == "__main__":
    parameters_only_entry_point(main)
