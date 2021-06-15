"""
WIP -- similar to the conda_job_script, this generates a
script that
"""

import os
import stat
from pathlib import Path
from typing import Optional

from attr import attrib, attrs
from attr.validators import instance_of, optional

from vistautils.parameters import Parameters
from vistautils.parameters_only_entrypoint import parameters_only_entry_point

from saga_tools.spack import SpackConfiguration


def main(params: Parameters):  # pragma: no cover
    docker_script_generator = DockerJobScriptGenerator.from_parameters(params)
    docker_image_name = params.string("docker_image_name")
    work_dir = params.optional_creatable_directory("working_directory") or Path(
        os.getcwd()
    )
    stdout_file = params.string("log_file") or work_dir / "___stdout.log"
    shell_script = docker_script_generator.generate_shell_script(
        docker_image_name=docker_image_name,
        working_directory=work_dir,
        stdout_file=stdout_file,
    )

    params.creatable_file("conda_script_path").write_text(  # type: ignore
        shell_script, encoding="utf-8"
    )

    if params.boolean("echo_template", default=False):
        print(shell_script)


@attrs(frozen=True, slots=True)
class DockerJobScriptGenerator:
    """
    WIP
    A driver to create a shell script in order to run a docker container.

    The program takes a parameters file as input to generate the executable script.
    The script can be
    used as an executable job for Pegasus.

    The generated script is based on
    https://github.com/isi-vista/saga-tools/blob/master/saga_tools/slurm_run_python.py
    with the slurm related configuration removed.

    ####################
    Docker Job Parameters
    ####################
    * *spack_environment*: (optional): the spack environment, if any, to run in.
        Cannot appear with *spack_packages*.
    * *spack_packages*: (optional): a YAML list of Spack packages to load
        (in *module@version* format).
    * *spack_root*: the spack installation to use (necessary only if *spack_environment*
        or *spack_modules*) is specified. This is usually the path to a working
        copy of a spack repository.
    * *docker_script_path*: the file to write the conda job script to.
    * *job_param_file* (optional): the param file to call
        the job with
    * *working_directory* (optional)
    """

    spack_config: Optional[SpackConfiguration] = attrib(
        validator=optional(instance_of(SpackConfiguration))
    )

    @staticmethod
    def from_parameters(params: Parameters) -> "DockerJobScriptGenerator":
        return DockerJobScriptGenerator(
            spack_config=SpackConfiguration.from_parameters(params)
        )

    def generate_shell_script(
        self,
        docker_image_name: str,
        *,
        cmd_args: str = None,
        working_directory: Path,
        stdout_file: Path,
        ckpt_path: Optional[Path] = None,
        pre_job: str = "",
        post_job: str = "",
    ) -> str:
        """
        Returns the content of a shell script to run a container from the
        given Docker image according to params

        If `ckpt_path` is provided this script will generate a `check point` to allow
        for future runs of the DAX to skip already generated outputs. This can be useful
        for outputs which are time or resource intensive or can be shared across projects.
        Examples: Running BERT on all of Gigaword or Converting RDF triples to FlexNLP documents
        """
        docker_job = DOCKER_JOB.format(
            docker_image=f"{docker_image_name}",
            docker_args=cmd_args,
            stdout_file=stdout_file,
        )
        ckpt_line = f"touch {ckpt_path.absolute()}" if ckpt_path else ""

        return DOCKER_SCRIPT.format(
            spack_lines=self.spack_config.sbatch_lines() if self.spack_config else "",
            working_directory=working_directory,
            docker_job=docker_job,
            ckpt_line="\n".join([f"echo {ckpt_line}", ckpt_line]),
            pre_job=pre_job,
            post_job=post_job,
        )

    def write_shell_script_to(
        self,
        docker_image_name: str,
        *,
        working_directory: Path,
        script_path: Path,
        cmd_args: str = None,
        stdout_file: Optional[Path] = None,
        ckpt_path: Optional[Path] = None,
        pre_job: str = "",
        post_job: str = "",
    ) -> None:

        if not stdout_file:
            stdout_file = working_directory / "___stdout.log"

        script_path.write_text(
            self.generate_shell_script(
                docker_image_name=docker_image_name,
                cmd_args=cmd_args,
                stdout_file=stdout_file,
                working_directory=working_directory,
                ckpt_path=ckpt_path,
                pre_job=pre_job,
                post_job=post_job,
            ),
            encoding="utf-8",
        )
        # Mark the generated script as executable.
        script_path.chmod(script_path.stat().st_mode | stat.S_IEXEC)


DOCKER_SCRIPT = """#!/usr/bin/env bash

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
{spack_lines}
cd {working_directory}
{pre_job}
{docker_job}
{post_job}
{ckpt_line}
"""

# DOCKER_JOB = """
# echo docker run {docker_args} {docker_image}
# docker run {docker_args} {docker_image} 2>&1 | tee {stdout_file}
# """

DOCKER_JOB = """
echo docker run {docker_args} {docker_image}
"""

if __name__ == "__main__":  # pragma: no cover
    parameters_only_entry_point(main)
