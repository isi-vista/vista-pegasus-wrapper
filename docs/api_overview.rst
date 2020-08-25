API Overview
============

The Pegasus code you’ll need to interact with is spread out over several
files, and it can be hard to figure out which parts are necessary. Parameters
in particular are a challenge. This document is intended as a guide to the
pieces you’ll need.

Functions
---------

To write your workflow generation script, you’ll want to use the
following functions:

-  To set things up, call ``initialize_vista_pegasus_wrapper(params)``

   -  Required before making any other calls to the wrapper.
   -  Note that the parameters must include some special wrapper-related
      parameters, detailed below.

-  To run jobs, use ``run_python_on_parameters()``. It returns a job
   identifier which can be used to specify dependencies between jobs. It
   takes the following arguments.

   -  ``name``, a ``Locator`` object.

      -  Used to compute the string name of the job as shown in the DAX
         and in SAGA
      -  Also defines where the job’s parameters, run script, and output
         will be stored relative to the workflow directory

         -  You can get this path using ``directory_for(locator)``, if
            for example you want to store other things there.

   -  ``module``

      -  Either an actual module (prevents errors at workflow generation
         time) or a string naming a module. This is your script. It
         should be a parameters-only entry point script created using
         ``vistautils``. The module must be installed in the conda
         environment specified by the workflow parameters.

   -  ``params``

      -  A parameters object holding the parameters that will be passed
         to the Python module when it is run.

         -  You’ll probably want to generate these programmatically;
            ``params.unify()`` is useful here.

   -  ``depends_on``

      -  **Required** and **keyword-only**.
      -  A collection (list, set, immutable set) of other jobs or files
         that this job depends on.
      -  To specify dependencies, you’ll want to store a reference to
         the required jobs somehow
         (``required_job = run_python_on_parameters(…)``) and later pass
         those references to the job that depends on them
         (``run_python_on_parameters(…, depends_on=[required_job])``).

   -  ``resource_request``

      -  *Optional*.
      -  Specifies backend-specific parameters, **overriding** any such
         parameters in ``params``.

         -  For example, using SLURM, how much memory, how many CPUs,
            how many GPUs, or what time limit Pegasus should request
            when starting the SLURM job.

-  To write the workflow, call ``write_workflow_description()``.

   -  This writes out the workflow graph, the submit script, and any
      other necessary files to the workflow directory.

Parameters
----------

The Pegasus wrapper requires a number of parameters to even function. In
addition there are several which, while not required, you might find
useful.

-  ``workflow_name``

   -  **Required**.
   -  This is the name that will appear when you run ``pegasus_status``.
      It’s also used when determining where the *run directory* goes.

-  ``workflow_directory``

   -  **Required**.
   -  This is where the workflow definition, submit script, and other
      related files will be stored.
   -  This is also where job parameters, run scripts, and outputs
      (stdout and stderr) are stored.

      -  They go in subdirectories determined by the job’s ``Locator``.

-  ``site``

   -  **Required**.
   -  This is a name for the place the workflow will run.

      -  Currently the only valid value is ``saga``.

-  ``namespace``

   -  **Required**.
   -  This is the default Pegasus namespace in which jobs are defined.

      -  This doesn’t affect anything because you can never end up with
         more than one namespace in your graph.

-  ``backend``

   -  **Required**.
   -  This is the name of the backend Pegasus will use to run the jobs.

      -  Currently the only valid value is ``slurm``.

-  User-specific parameters, typically specified in ``root.params``

   -  ``spack_root``

      -  *Optional*.
      -  Specifies the ``spack`` root.

   -  ``conda_base_path``

      -  **Required**.
      -  Specifies the path to your Anaconda or Miniconda install

         -  **NOT** your project-specific environment

      -  Example: ``/nas/home/jcecil/Miniconda3``

   -  ``conda_environment``

      -  The name of the conda environment you want to use when running
         the jobs

-  SLURM-specific backend parameters

   -  ``partition``

      -  **Required** when using SLURM as backend.
      -  The partition to run on.
      -  Note that there is no ``account`` parameter and no ``qos``
         parameter.

         -  The wrapper infers the correct account or QOS to specify
            based on the partition name. If you specify
            ``partition: mics`` then it will specify
            ``--partition mics`` :literal:`--``account mics`. Likewise,
            if you specify ``partition: ephemeral`` then it will specify
            ``--partition ephemeral`` :literal:`--``qos ephemeral`.
         -  You can’t override the inferred account or QOS.

   -  ``num_cpus``

      -  *Optional*.
      -  Passed to SLURM using ``--cpus-per-task``.

   -  ``num_gpus``

      -  *Optional*.
      -  Passed to SLURM using ``--gpus-per-task``.

   -  ``memory``

      -  *Optional*.
      -  Passed to SLURM using ``--mem``.
      -  This should be a memory string much like you'd specify to SLURM
         directly. For example, ``memory: '4G'``.

   -  ``job_time_in_minutes``

      -  *Optional*.
      -  The wall clock time limit in minutes. Must be an integer.
      -  Must make sure that this parameter matches the partition
         specified. If you specify a job time larger than the partition
         allows, the job will fail (not always with the most helpful
         error message).
