############
User Mapping
############

About
-----

For Autosubmit, user mapping is the process of associating all the personal user accounts with a shared account.

The personal user account is the account used to access to each remote platform. While the shared account is the one that is used to run the experiments on the machine where Autosubmit is deployed.

When to use
-----------

To run a set of shared experiments on a machine where Autosubmit is deployed. Using the HPC resources of the user that triggers the experiment.

More specifically, this can be useful to launch something like an experiment testing suite on a shared machine without having redundancy of experiments for each user that wants to run the tests.

Prerequisites
--------------

* The sysadmin of the machine where Autosubmit is deployed must have created a shared user account that will be used to run the experiments.

* The sysadmin is the responsible for handling the security of the remote keys used so that the personal user accounts are not compromised.

* The user is reponsible for keeping their personal user account details (e.g SSH keys) secure, e.g. not sharing them with others.

* The ``platform_${SUDO_USER}.yml`` file for each user that has access to the shared account must be created.

* The ``ssh_config_${SUDO_USER}`` file for each user that has access to the shared account must be created.

How it works
------------

The idea is to map two different stuff depending on the user that is logged in the shared account to ensure a correct Autosubmit behavior.

* Platform.yml file that contains the personal user for each platform.

(Personal user action): The user must set some enviroment variables "AS_ENV_PLATFORMS_PATH" to point to a file that contains the personal platforms.yml file.

Defaults to: None

(One time, all shared experiments): Has to have this defined in the $autosubmit_data/$expid/conf

.. code-block:: yaml

    ...
    DEFAULT:
        ...
        CUSTOM_CONFIG:
            ...
            POST: %AS_ENV_PLATFORMS_PATH%
        ...
    ...


* (OPTIONAL) ssh_config file that contains the ssh config for each platform

(Personal user action): Must set some enviroment variables "AS_ENV_SSH_CONFIG_PATH" to point to a file that contains the personal ~/.ssh/config file.

Defaults to: "~/.ssh/config" or "~/.ssh/config_${SUDO_USER}" if the env variable: "AS_ENV_PLATFORMS_PATH" is set.


How to activate it with examples
--------------------------------

* (once) Generate the platform_${SUDO_USER}.yml

.. code-block:: yaml

    Platforms:
        Platform:
            User: bscXXXXX

* (once) Generate the ssh_config_${SUDO_USER}.yml

.. code-block:: ini

    Host marenostrum5
        Hostname glogin1.bsc.es
        User bscXXXXX
    Host marenostrum5.2
        Hostname glogin2.bsc.es
        User bscXXXXX

1) Set the environment variable "AS_ENV_PLATFORMS_PATH".

.. code-block:: bash

    export AS_ENV_PLATFORMS_PATH="~/platforms/platform_${SUDO_USER}.yml"

Tip: Add it to the shared account .bashrc file.

2) Set the environment variable "AS_ENV_SSH_CONFIG_PATH" (OPTIONAL).

.. code-block:: bash

    export AS_ENV_SSH_CONFIG_PATH="~/ssh/config_${SUDO_USER}.yml"

Tip: Add it to the shared account .bashrc file.

3) Ensure that the experiments has set the %CUSTOM_CONFIG.POST% to the "AS_ENV_PLATFORMS_PATH" variable.

.. code-block:: bash

    cat $autosubmit_data/$expid/conf/minimal.yml

.. code-block:: yaml

    ...
    DEFAULT:
        ...
        CUSTOM_CONFIG:
            ...
            POST: %AS_ENV_PLATFORMS_PATH%
        ...
    ...

4) Run the experiments.

.. code-block:: bash

    autosubmit run $expid
