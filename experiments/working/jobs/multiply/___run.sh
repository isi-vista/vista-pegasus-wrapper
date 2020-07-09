#!/usr/bin/env bash

set -e

# This is needed because SLURM jobs are run from a non-interactive shell,
# but conda expects PS1 (the prompt variable) to be set.
if [[ -z ${PS1+x} ]]
  then
    export PS1=""
fi

source "/nas/home/hhasan/miniconda3"/etc/profile.d/conda.sh
conda activate event-gpu-py36


cd /nas/gaia/users/hhasan/Projects/vista-pegasus-wrapper/experiments/working/jobs/multiply
echo `which python`
echo python -m pegasus_wrapper.scripts.multiply_by_x /nas/gaia/users/hhasan/Projects/vista-pegasus-wrapper/experiments/working/jobs/multiply/____params.params
python -m pegasus_wrapper.scripts.multiply_by_x /nas/gaia/users/hhasan/Projects/vista-pegasus-wrapper/experiments/working/jobs/multiply/____params.params | tee /nas/gaia/users/hhasan/Projects/vista-pegasus-wrapper/experiments/working/jobs/multiply/___stdout.log
touch /nas/gaia/users/hhasan/Projects/vista-pegasus-wrapper/experiments/working/jobs/multiply/___ckpt
