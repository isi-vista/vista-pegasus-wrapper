import os
import stat
from pathlib import Path
from typing import Optional, Union

from attr import attrib, attrs
from attr.validators import instance_of, optional

from vistautils.io_utils import CharSink
from vistautils.parameters import Parameters, YAMLParametersWriter
from vistautils.parameters_only_entrypoint import parameters_only_entry_point

from saga_tools.conda import CondaConfiguration
from saga_tools.spack import SpackConfiguration


def main(params: Parameters):
    conda_script_generator = CondaJobScriptGenerator.from_parameters(params)
    entry_point = params.string("entry_point")
    shell_script = conda_script_generator.generate_shell_script(
        entry_point_name=entry_point,
        param_file=params.existing_file("job_param_file"),
        working_directory=params.optional_creatable_directory("working_directory")
        or Path(os.getcwd()),
    )

    params.creatable_file("conda_script_path").write_text(  # type: ignore
        shell_script, encoding="utf-8"
    )

    if params.boolean("echo_template", default=False):
        print(shell_script)


@attrs(frozen=True, slots=True)
class CondaJobScriptGenerator:
    """
    A driver to create a shell script in order to run a python program
    in a conda virtual environment.

    The program takes a parameters file as input to generate the executable script.
    The script can be
    used as an executable job for Pegasus
    so that a job can be run in a venv
    as the default pegasus jobs do not run in one.

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
    * *spack_packages*: (optional): a YAML list of Spack packages to load
        (in *module@version* format).
    * *spack_root*: the spack installation to use (necessary only if *spack_environment*
        or *spack_modules*) is specified. This is usually the path to a working
        copy of a spack repository.
    * *entry_point*: the name of the module to run, e.g. vistautils.scripts.foo
    * *conda_script_path*: the file to write the conda job script to.
    * *job_param_file* (optional): the param file to call
        the job with
    * *working_directory* (optional)
    """

    conda_config: Optional[CondaConfiguration] = attrib(
        validator=optional(instance_of(CondaConfiguration))
    )
    spack_config: Optional[SpackConfiguration] = attrib(
        validator=optional(instance_of(SpackConfiguration))
    )

    @staticmethod
    def from_parameters(params: Parameters) -> "CondaJobScriptGenerator":
        return CondaJobScriptGenerator(
            conda_config=CondaConfiguration.from_parameters(params),
            spack_config=SpackConfiguration.from_parameters(params),
        )

    def generate_shell_script(
        self, entry_point_name: str, param_file: Path, *, working_directory: Path
    ):

        return CONDA_SCRIPT.format(
            conda_lines=self.conda_config.sbatch_lines() if self.conda_config else "",
            spack_lines=self.spack_config.sbatch_lines() if self.spack_config else "",
            working_directory=working_directory,
            entry_point=entry_point_name,
            param_file=param_file,
        )

    def write_shell_script_to(
        self,
        entry_point_name: str,
        parameters: Union[Path, Parameters],
        *,
        working_directory: Path,
        script_path: Path,
        params_path: Optional[Path],
    ) -> None:
        if isinstance(parameters, Path):
            if params_path:
                raise RuntimeError(
                    "Cannot specify params_path and provide a path for parameters"
                )
            params_path = parameters
        elif isinstance(parameters, Parameters):
            if not params_path:
                raise RuntimeError(
                    "Params path must be specified when providing a parameters object"
                )
            YAMLParametersWriter().write(parameters, CharSink.to_file(params_path))
        else:
            raise RuntimeError(
                f"Parameters must be either Parameters or path to a param file, "
                f"but got {parameters}"
            )

        script_path.write_text(
            self.generate_shell_script(
                entry_point_name=entry_point_name,
                param_file=params_path,
                working_directory=working_directory,
            ),
            encoding="utf-8",
        )
        # Mark the generated script as executable.
        script_path.chmod(script_path.stat().st_mode | stat.S_IEXEC)


CONDA_SCRIPT = """#!/usr/bin/env bash

set -e

# This is needed because SLURM jobs are run from a non-interactive shell,
# but conda expects PS1 (the prompt variable) to be set.
if [[ -z ${{PS1+x}} ]]
  then
    export PS1=""
fi
{conda_lines}
{spack_lines}
cd {working_directory}
echo `which python`
echo python -m {entry_point} {param_file}
python -m {entry_point} {param_file}
"""

if __name__ == "__main__":
    parameters_only_entry_point(main)
