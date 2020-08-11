<!-- 
[![Build status](https://ci.appveyor.com/api/projects/status/3jhdnwreqoni1492/branch/master?svg=true)](https://ci.appveyor.com/project/isi-vista/vista-pegasus-wrapper/branch/master) 
-->
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
See [example_workflow](scripts/example_workflow.py) for an extremely simple workflow which we will use to demonstrate the process.
To see the example workflow add a `root.params` file to the parameters directory with the following:
*Note the Directory should be in your $Home and not a NFS like /nas/gaia/ as the submission will fail for an NFS reason*
```
example_root_dir: "path/to/output/dir/"
```
run `python -m pegasus_wrapper.example_workflow_builder parameters/root.params` from this project's root folder.

The log output will provide you the output location of the `Text.dax` Assuming you are logged into a submit node with an active Pegasus install:

```
cd "path/to/output/dir"
pegasus-plan --conf pegasus.conf --dax Test.dax --dir "path/to/output/dir" --relative-dir exampleRun-001
pegasus-run "path/to/output/dir/"exampleRun-001
```
The example workflow submits **ONLY** to `scavenge`. In an actual workflow we would recommend parameterizing it.

Our current system places `ckpt` files to indicate that a job has finished in the event the DAX needs to be generated again to fix a bug after an issue was found. This system is non-comprehensive as it currently requires manual control. When submitting a new job using previous handles use a new relative dir in the plan and run.

A [Nuke Checkpoints](scripts/nuke_checkpoints.py) script is provided for ease of removing checkpoint files. To use, pass a directory location as the launch parameter and the script will remove checkpoint files from the directory and all sub-directories.

# FAQ
## How can I exclude some nodes?

You can use the exclude_list parameter as a workflow parameter (or resource parameter) to exclude some nodes. This is useful if some nodes are not properly configured or donot have the features you expect but otherwise are ready to be used.

## How can I run my pipeline on one specific node?
Use run_on_single_node parameter when you initialize a workflow (or a Slurm resource) to specify a single node to run your pipeline (or a single step of your pipeline) on a single node. 
* Note you cannot use this option with the **exclude_list** option.
* Note you cannot specify more than one node using this option.

## What are valid root directories for the workflow?

Currently the root directory should be be in your home directory and not on an NAS like `/nas/gaia/` as the submission will fail for an NFS reason.
The experiment directory can be (and ought to be) on such a drive, though.

# Common Errors

## Mismatching partition selection and max walltime

Partitions each have a max walltime associated with them. See the saga cluster wiki [here]("https://github.com/isi-vista/saga-cluster/wiki/How-to-use-the-SAGA-queue#partitions"). If you specify a partition with a `job_time_in_minutes` greater than that partition's max walltime, you will see an error. 

# Contributing

Run `make precommit` before commiting.  

If you are using PyCharm, please set your docstring format to "Google" and your unit test runner to "PyTest"
in `Preferences | Tools | Python Integrated Tools`.
