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

# Contributing

Run `make precommit` before commiting.  

If you are using PyCharm, please set your docstring format to "Google" and your unit test runner to "PyTest"
in `Preferences | Tools | Python Integrated Tools`.

# Usage

Using [WorkflowBuilder from `workflow.py`](pegasus_wrapper/workflow.py) develop a function to generate a `Workflow.dax` See [example_workflow](scripts/example_workflow.py) for an extremely simple workflow.
To see the example workflow add a `root.params` file to the parameters directory with the following: *Note the Directory should be in your $Home and not a NFS like /nas/gaia/ as the submission will fail for an NFS reason*
```
dir: "path/to/output/dir/"
```
run `python -m pegasus_wrapper.example_workflow_builder parameters/root.params` from this project's root folder.

The log output will provide you the output location of the `Text.dax` Assuming you are logged into a submit node with an active Pegasus install:

```
cd "path/to/output/dir"
pegasus-plan --conf pegasus.conf --dax Test.dax --dir "path/to/output/dir" --relative-dir exampleRun-001 --cleanup leaf --force --sites saga --output-site local
pegasus-run "path/to/output/dir/"exampleRun-001
```
The example workflow submits **ONLY** to scavenge in an actual workflow we would recommend parameterizing it.

Our current system places `ckpt` files to indicate that a job has finished in the event the DAX needs to be generated again to fix a bug after an issue was found. This system is non-comprehensive as it currently requires manual control. When submitting a new job using previous handles use a new relative dir in the plan and run.

A [Nuke Checkpoints](scripts/nuke_checkpoints.py) script is provided for ease of removing checkpoint files. To use, pass a directory location as the launch parameter and the script will remove checkpoint files from the directory and all sub-directories.
