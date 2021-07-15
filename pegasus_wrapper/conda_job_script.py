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


def main(params: Parameters):  # pragma: no cover
    conda_script_generator = CondaJobScriptGenerator.from_parameters(params)
    entry_point = params.string("entry_point")
    work_dir = params.optional_creatable_directory("working_directory") or Path(
        os.getcwd()
    )
    stdout_file = params.string("log_file") or work_dir / "___stdout.log"
    shell_script = conda_script_generator.generate_shell_script(
        entry_point_name=entry_point,
        param_file=params.existing_file("job_param_file"),
        working_directory=work_dir,
        stdout_file=stdout_file,
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
        self,
        *,
        entry_point_name: str = None,
        param_file: Path = None,
        python_path: Path = None,
        cmd_args: str = None,
        working_directory: Path,
        stdout_file: Path,
        ckpt_path: Optional[Path] = None,
        override_conda_config: Optional[CondaConfiguration] = None,
        python: str = "python",
        pre_job: str = "",
        post_job: str = "",
    ) -> str:
        """
        Returns the content of a shell script to run the given Python entry point
        in a conda virtual environment according to params

        If `ckpt_path` is provided this script will generate a `check point` to allow
        for future runs of the DAX to skip already generated outputs. This can be useful
        for outputs which are time or resource intensive or can be shared across projects.
        Examples: Running BERT on all of Gigaword or Converting RDF triples to FlexNLP documents
        """
        if entry_point_name and python_path:
            raise RuntimeError(
                f"Can not provide both a python path ({python_path}) and an entry point name ({entry_point_name}) to job shell script generation."
            )
        if param_file and cmd_args:
            raise RuntimeError(
                f"Can not provide both a parameters file and command line args.\n"
                f"Param: {param_file}\n"
                f"Cmds: {cmd_args}"
            )
        if override_conda_config:
            conda_config = override_conda_config
        else:
            conda_config = self.conda_config

        python_job = PYTHON_JOB.format(
            path_or_entry_point=f"-m {entry_point_name}"
            if entry_point_name
            else python_path,
            param_file_or_args=param_file if param_file else cmd_args,
            python=python,
            stdout_file=stdout_file,
        )
        ckpt_line = f"touch {ckpt_path.absolute()}" if ckpt_path else ""

        return CONDA_SCRIPT.format(
            conda_lines=conda_config.sbatch_lines() if conda_config else "",
            spack_lines=self.spack_config.sbatch_lines() if self.spack_config else "",
            working_directory=working_directory,
            python_job=python_job,
            ckpt_line="\n".join([f"echo {ckpt_line}", ckpt_line]),
            pre_job=pre_job,
            post_job=post_job,
        )

    def write_shell_script_to(
        self,
        entry_point_name: Union[str, Path],
        parameters: Union[Path, Parameters, str],
        *,
        working_directory: Path,
        script_path: Path,
        params_path: Optional[Path],
        stdout_file: Optional[Path] = None,
        ckpt_path: Optional[Path] = None,
        override_conda_config: Optional[CondaConfiguration] = None,
        python: str = "python",
        treat_params_as_cmd_args: bool = False,
        pre_job: str = "",
        post_job: str = "",
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
        elif isinstance(parameters, str):
            if not treat_params_as_cmd_args:
                raise RuntimeError(
                    "Parameters can only be a str when the parameters are being treated as command line args"
                )
        else:
            raise RuntimeError(
                f"Parameters must be either Parameters, path to a param file, "
                f"or a string if treat_params_as_cmd_args is True, "
                f"but got {parameters}"
            )

        if not stdout_file:
            stdout_file = working_directory / "___stdout.log"

        script_path.write_text(
            self.generate_shell_script(
                entry_point_name=entry_point_name
                if isinstance(entry_point_name, str)
                else None,
                python_path=str(entry_point_name.absolute()).strip()
                if isinstance(entry_point_name, Path)
                else None,
                param_file=params_path if not treat_params_as_cmd_args else None,
                cmd_args=parameters.strip() if treat_params_as_cmd_args else None,
                stdout_file=stdout_file,
                working_directory=working_directory,
                ckpt_path=ckpt_path,
                override_conda_config=override_conda_config,
                python=python,
                pre_job=pre_job,
                post_job=post_job,
            ),
            encoding="utf-8",
        )
        # Mark the generated script as executable.
        script_path.chmod(script_path.stat().st_mode | stat.S_IEXEC)


CONDA_SCRIPT = """#!/usr/bin/env bash

set -e
# This is needed so the output redirect for the Python command doesn't
# suppress the exit code of the Python process itself.
set -o pipefail

# This is needed because SLURM jobs are run from a non-interactive shell,
# but conda expects PS1 (the prompt variable) to be set.
if [[ -z ${{PS1+x}} ]]
  then
    export PS1=""
fi
{conda_lines}
{spack_lines}
cd {working_directory}
{pre_job}
{python_job}
{post_job}
{ckpt_line}
"""

PYTHON_JOB = """echo `which {python}`
echo {python} {path_or_entry_point} {param_file_or_args}
{python} {path_or_entry_point} {param_file_or_args} 2>&1 | tee {stdout_file}
"""

if __name__ == "__main__":  # pragma: no cover
    parameters_only_entry_point(main)
