# Copyright 2015-2023 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

"""Documentation generator."""

from os import linesep

from typing import Any, Dict

from autosubmitconfigparser.config.configcommon import AutosubmitConfig


def _print_jobs_docs(expid: str, jobs: Dict[str, Any]) -> None:
    """Print the documentation for the jobs.

    :param jobs: A dictionary with the job section and the job values.
    :type jobs: Dict[str, Any]
    :return: Nothing.
    :rtype: None
    """
    # TODO: use some sort of template -- later
    print(f'# Workflow Jobs{linesep}')
    # TODO: add description
    print(f'These are the jobs in the workflow “{expid}{linesep}”.')

    for job_section, job_dict in jobs.items():
        print(f'## {job_section}{linesep}')
        if {'DOC', 'TITLE'}.intersection(job_dict.keys()):
            title = job_dict['TITLE']
            doc = job_dict['DOC']

            print(f'**{title}**')
            print(f'{doc}{linesep}')


def generate_documentation(as_conf: AutosubmitConfig):
    """Automated documentation generation for Autosubmit experiments.

    Goes over experiment configuration, gathering each job's
    ``TITLE`` and ``DOC`` to create the documentation for the
    jobs.

    Goes over the ``METADATA`` configuration entry, looking
    for sections with ``key=value`` pairs, or links to
    configuration keys, e.g.:

    .. code-block:: yaml

      METADATA:
        MODEL:
          # This is a static value that will produce name = IFS
          - name: name
          # This is a dynamic value that will produce resolution = abc...
          - resolution: %RUN.IFS.RESOLUTION%
          # This resolves the key and value automatically and will produce RUN.IFS.INIPATH=/path/path/...
          - %RUN.IFS.INIPATH%
          # This is the extended format
          - name: source
            value: https://git@...
            documentation: |
              The source code is hosted at the private repository...
        DATA:
          - PROVENANCE: ...


    Each property of the ``METADATA`` YAML object is rendered as
    a separate section. These sections, in YAML, are arrays that
    contain metadata references in each line. Extra documentation
    about each key can be entered using the extended format.

    :param as_conf: Autosubmit configuration
    :type as_conf: AutosubmitConfig
    :return: Nothing.
    :rtype: None
    """

    # TODO: expand variables (like %EXPID%); forgot how that is done -- check with Dani later
    _print_jobs_docs(as_conf.expid, as_conf.jobs_data)

    # _print_jobs_metadata(as_conf)
    # N.B. These keys must be defined by experiment experts (e.g. workflow devs, app devs, users, etc.).
    #      Our job here is just on the automation side. We must try to keep it simple and
    #      transparent as this reflects on reproducibility and traceability in the experiment
    #      and any customization or logic-heavy work here could make it harder to confirm
    #      the tool is not responsible for some value that could be incorrect in the report!
    print('# Metadata')

    # TODO: fetch the contents of METADATA, and do as the docstring describes


___all__ = [
    'generate_documentation'
]
