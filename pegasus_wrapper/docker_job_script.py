import stat
from pathlib import Path
from typing import Optional

from attr import attrib, attrs
from attr.validators import instance_of, optional

from vistautils.parameters import Parameters

from saga_tools.spack import SpackConfiguration


@attrs(frozen=True, slots=True)
class DockerJobScriptGenerator:
    """
    A driver to create a shell script in order to run a docker container.

    The program takes a parameters file as input to generate the executable script.
    The script can be used as an executable job for Pegasus.

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
    """

    spack_config: Optional[SpackConfiguration] = attrib(
        validator=optional(instance_of(SpackConfiguration))
    )

    @staticmethod
    def from_parameters(params: Parameters) -> "DockerJobScriptGenerator":
        return DockerJobScriptGenerator(
            spack_config=SpackConfiguration.from_parameters(params)
        )

    def write_shell_script_to(
        self,
        docker_image_name: str,
        docker_command: str,
        docker_tar_path: str,
        *,
        working_directory: Path,
        script_path: Path,
        cmd_args: str = None,
        ckpt_path: Optional[Path] = None,
        pre_job: str = "",
        post_job: str = "",
    ) -> None:
        script_path.write_text(
            self.generate_shell_script(
                docker_image_name=docker_image_name,
                docker_command=docker_command,
                docker_tar=docker_tar_path,
                cmd_args=cmd_args,
                working_directory=working_directory,
                ckpt_path=ckpt_path,
                pre_job=pre_job,
                post_job=post_job,
            ),
            encoding="utf-8",
        )
        # Mark the generated script as executable.
        script_path.chmod(script_path.stat().st_mode | stat.S_IEXEC)

    def write_service_shell_script_to(
        self,
        docker_container_name: str,
        docker_image_path: str,
        docker_args: str,
        *,
        start_script_path: Path,
        stop_script_path: Path,
        remove_docker_on_exit: bool = True,
    ) -> None:

        start_script_path.write_text(
            self.start_docker_script_text(
                docker_container_name=docker_container_name,
                docker_args=docker_args,
                docker_img=docker_image_path,
                remove_on_exit=remove_docker_on_exit,
            ),
            encoding="utf-8",
        )

        stop_script_path.write_text(
            self.stop_docker_script_text(docker_container_name=docker_container_name)
        )
        # Mark the generated script as executable.
        start_script_path.chmod(start_script_path.stat().st_mode | stat.S_IEXEC)
        stop_script_path.chmod(stop_script_path.stat().st_mode | stat.S_IEXEC)

    def generate_shell_script(
        self,
        docker_image_name: str,
        docker_command: str,
        docker_tar: str,
        *,
        cmd_args: str = "",
        working_directory: Path,
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
            docker_command=docker_command,
            docker_tar=docker_tar,
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

    def start_docker_script_text(
        self,
        docker_container_name: str,
        docker_img: str,
        docker_args: str,
        *,
        remove_on_exit: bool = True,
    ) -> str:
        return DOCKER_START_SCRIPT.format(
            docker_container_name=docker_container_name,
            docker_img=docker_img,
            args=docker_args,
            remove=" --rm " if remove_on_exit else "",
        )

    def stop_docker_script_text(self, docker_container_name: str) -> str:
        return DOCKER_STOP_SCRIPT.format(docker_container_name=docker_container_name)


DOCKER_SCRIPT = """#!/usr/bin/env bash
set -e
# This is needed so the output redirect for the Python command doesn't
# suppress the exit code of the Python process itself.
set -o pipefail

{spack_lines}
cd {working_directory}
{pre_job}
{docker_job}
{post_job}
{ckpt_line}
"""

DOCKER_JOB = """
echo docker load --input {docker_tar}
docker load --input {docker_tar}
echo docker run {docker_args} {docker_image} {docker_command}
docker run {docker_args} {docker_image} {docker_command}
"""

DOCKER_STOP_SCRIPT = """
#!/bin/bash

docker stop {docker_container_name}
echo "Stopped {docker_container_name}"
"""

DOCKER_START_SCRIPT = """
#!/bin/bash -l

echo 'Checking for existing container...'
RESULT=`docker container inspect -f '{{.Name}} {{.Id}} {{.State.Status}}' {docker_container_name}`
if [[ -z "$RESULT" ]]; then
  echo 'Starting...'
  docker run --name {docker_container_name} -d {args}{remove} {docker_img}
  echo '{docker_container_name} is up'
else
  echo '{docker_container_name} is already up'
fi
"""
