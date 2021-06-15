[![Build status](https://travis-ci.com/isi-vista/vista-pegasus-wrapper.svg?branch=master)](https://travis-ci.com/isi-vista/vista-pegasus-wrapper?branch=master)
[![codecov](https://codecov.io/gh/isi-vista/vista-pegasus-wrapper/branch/master/graph/badge.svg)](https://codecov.io/gh/isi-vista/vista-pegasus-wrapper)

# Documentation

To generate documentation:
```
cd docs
make html
```

The docs will be under `docs/_build/html`

# Project Setup

1. Create a Python 3.6 Anaconda environment (or your favorite other means of creating a virtual environment): `conda create --name pegasus-wrapper python=3.6` followed by `conda activate pegasus-wrapper`.
2. `pip install -r requirements.txt`

# Usage
A workflow is an organized logical path taking input data and running predefined processes over the data to produce a structured output.
[Pegasus](https://pegasus.isi.edu/) is an ISI developed workflow management system which is used to manage the submission and execution of these workflows. 
Pegasus functions on a directed graph structure (a *DAX*) to manage the job dependency, file transfer between computational resources, and execution environments.
This library simplifies the process of writing a profile which can be converted into a DAX for submission to the Pegasus controller.

Using [WorkflowBuilder from `workflow.py`](pegasus_wrapper/workflow.py) develop a function to generate a `Workflow.dax`.
See [example_workflow](pegasus_wrapper/scripts/example_workflow_builder.py) for an extremely simple workflow which we will use to demonstrate the process.
To see the example workflow add a `root.params` file to the parameters directory with the following:
```
example_root_dir: "path/to/output/dir/"
conda_environment: "pegasus-wrapper"
conda_base_path: "path/to/conda"
```
run `python -m pegasus_wrapper.scripts.example_workflow_builder parameters/root.params` from this project's root folder.

The log output will provide you the output location of the `Text.dax` Assuming you are logged into a submit node with an active Pegasus install:

```
cd "path/to/output/dir"
./submit.sh
```
The example workflow submits **ONLY** to `scavenge`. In an actual workflow we would recommend parameterizing it.

Our current system places `ckpt` files to indicate that a job has finished in the event the DAX needs to be generated again to fix a bug after an issue was found. This system is non-comprehensive as it currently requires manual control. When submitting a new job using previous handles use a new relative dir in the plan and run.

A [Nuke Checkpoints](scripts/nuke_checkpoints.py) script is provided for ease of removing checkpoint files. To use, pass a directory location as the launch parameter and the script will remove checkpoint files from the directory and all sub-directories.

It is recommended to use a shared directory on the NAS, e.g. `/nas/gaia` to host a workflow under as compared to a users `/nas/user/xyz` home directory due to space limitations on the NAS.

Currently, all workflows will run in the `sharedfs` data staging configuration.

# FAQ
## How can I exclude some nodes?

You can use the exclude_list parameter as a workflow parameter (or resource parameter) to exclude some nodes. This is useful if some nodes are not properly configured or donot have the features you expect but otherwise are ready to be used.

## How can I run my pipeline on one specific node?
Use run_on_single_node parameter when you initialize a workflow (or a Slurm resource) to specify a single node to run your pipeline (or a single step of your pipeline) on a single node. 
* Note you cannot use this option with the **exclude_list** option.
* Note you cannot specify more than one node using this option.

## How can I limit the number of active jobs at one time?
Pegasus is powered by HTCondor which allows for limiting the number of active jobs in a given category. By default, jobs requested for the SAGA cluster are automatically categorized according to their partition. Custom category names can be provided during job creation.
To limit the number of active jobs in a category call `limit_jobs_for_category(category: str, max_jobs: int)` where `category` is the string of the category to limit and the `max_jobs` is the maximum number of jobs to permit. We recommend providing a parameter setting to configure the `max_jobs` value.

# Common Errors

## Mismatching partition selection and max walltime

Partitions each have a max walltime associated with them. See the saga cluster wiki [here]("https://github.com/isi-vista/saga-cluster/wiki/How-to-use-the-SAGA-queue#partitions"). If you specify a partition with a `job_time_in_minutes` greater than that partition's max walltime, you will see an error. 

## Modifying code while pipeline runs

If you change code while a pipeline is runnning, the jobs will pick up the changes. This could be helpful if you notice an error and fix it before that code runs, but can also lead to some unexpected behavior.

## `No module named 'Pegasus'` (Version 4.9.3)
*This is believed to have been fixed for Pegasus Version 5. If this arises please leave an issue*

## Debugging from `srun` fails to load `cuda` and `cudnn`

A new node gotten with `srun` does not load the Spack modules you usually have set up in your runtime scripts. You need to manually install these if you want to work with Tensorflow or anything requiring Cuda.

# Updating from wrapper script to use Pegasus5.0.0 from Pegasus4.9.3

No changes should be needed for any project using the previous version of the wrapper which supported Pegasus4.9.3.

# Contributing

Run `make precommit` before commiting.  

If you are using PyCharm, please set your docstring format to "Google" and your unit test runner to "PyTest"
in `Preferences | Tools | Python Integrated Tools`.
