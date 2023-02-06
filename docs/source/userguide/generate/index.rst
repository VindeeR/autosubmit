Generate Workflow Configuration
===============================

By default, Autosubmit produces an internal representation of a workflow to run
the experiment created and configured. With the ``autosubmit generate`` subcommand,
it is possible to generate an external configuration of the same workflow in a
different syntax, for a different workflow backend engine.

At the moment, the only workflow engine supported is `ecFlow <https://ecflow.readthedocs.io/>`_.

Prerequisites
-------------

Before running ``autosubmit generate``, you **must** have executed ``autosubmit create``.
This is important as the ``create`` subcommand produces the information necessary to have
a workflow graph. This graph is traversed by the ``generate`` subcommand to produce a new
graph for a different workflow engine.

How to generate workflow configuration
--------------------------------------

The command syntax is:

.. code-block:: bash

    autosubmit generate <expid> --engine <engine> args...

PyFlow / ecFlow
~~~~~~~~~~~~~~~

`PyFlow <https://pyflow-workflow-generator.readthedocs.io/>`_ is a Python utility developed
by the ECMWF to generate workflow configuration for workflow engines. At the moment its only
output workflow configuration is for ecFlow, an ECMWF workflow manager.

.. code-block:: bash
   :caption: Command usage to generate PyFlow/ecFlow configuration

    autosubmit generate <expid> \
        --generate {pyflow} \
        --server=<string> \
        --output=<string> \
        [--quiet --deploy --port=<int>]

For PyFlow/ecFlow, the required parameters are the ``--server`` where the workflow
will run, and the ``--output`` with the directory to write the ecFlow generated
files. If you enable ``--deploy``, it will call code from PyFlow to deploy it to
ecFlow. For this option, you will also have to specify ``--port``.

To reduce verbosity of the command, you can specify ``--quiet``, although that does not
guarantee complete the command will not output anything — as it calls other modules.

Scripts preprocessing
---------------------

One important thing to keep in mind when generating workflow configurations for different
workflow engines, is the use of preprocessing of script templates.

Autosubmit, as many other workflow managers, offers a variable substitution (interpolation)
that is used to preprocess task scripts. For example:

.. code-block:: bash
   :caption: Task script that require Autosubmit preprocessing

    echo "The root dir is %ROOTDIR%"

The ``ROOTDIR`` variable is :doc:`replaced by Autosubmit </devguide/variables>`, before Bash shell executes the script
(i.e. it is not an environment variable). The ``ROOTDIR`` is a variable provided by the
Autosubmit runtime, that may exist in other workflow managers, but it may have a different
name.

This is a problem for the portability of the generated scripts. A recommended workaround
for this issue is to use a single script that defines the variables used by the workflow
tasks. For example, a file called ``prepare_environment.sh``:

.. code-block:: bash
   :caption: ``prepare_environment.sh`` for Autosubmit

    ROOTDIR=%ROOTDIR%
    CHUNK=%CHUNK%

This script will have to ``source`` that script in your Bash scripts, like so:

.. code-block:: bash
   :caption: Task script that does not require Autosubmit preprocessing

    #!/bin/bash
    set -xuve

    source prepare_environment.sh

    echo "The root dir is ${ROOTDIR}"

The idea of this approach is to reduce the necessary modifications when porting
the workflow from Autosubmit to a different workflow engine. In contrast, if you
used the Autosubmit variables in all your template files, that means that when
porting to a different workflow engine you would have to ``a)`` adjust every
script to use the correct variables, or ``b)`` preprocess the scripts with
Autosubmit assuming you have an identical target platform, or ``c)`` change the
generated workflow configuration manually.

In the case of PyFlow/ecFlow, for instance, the ``prepare_environment.sh`` file
would have to be updated to use the correct variable substitution syntax and the
correct ecFlow variable. For example:

.. code-block:: bash
   :caption: ``prepare_environment.sh`` modified for ecFlow

    ROOTDIR=%ECF_HOME%  # ECF_HOME is a possible replacement for ROOTDIR
    CHUNK=%CHUNK%       # CHUNK is set by the generate subcommand via PyFlow

.. note:: Note
   Autosubmit and ecFlow have similar syntax for the variables that are preprocessed,
   using ``%`` to wrap the variables. However, this may not be always the case. You can
   find workflow managers that use other symbols, or Jinja, YAML input files, etc.
