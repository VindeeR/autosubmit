import argparse
import os
import re
import tempfile
from enum import Enum
from typing import List

import pyflow as pf
from pyflow import *

from autosubmit.job.job_list import JobList, Job

# Pattern used to verify if a TASK name includes the previous CHUNK number, with a separator.
PREVIOUS_CHUNK_PATTERN = re.compile(r'''
    ([a-zA-Z0-9_\-\.]+) # The Task name (e.g. TASK);
    -                   # TASK and CHUNK separator, i.e. TASK-1 (the hyphen between TASK and 1);
    ([\d]+)             # The Chunk name (e.g. 1).
''', re.X)

# Autosubmit Task name separator (not to be confused with task and chunk name separator).
DEFAULT_SEPARATOR = '_'


class Running(Enum):
    """The Running level of an Autosubmit task."""
    ONCE = 'once'
    MEMBER = 'member'
    CHUNK = 'chunk'
    SPLIT = 'split'

    def __str__(self):
        return self.value


# TODO: split?
# Defines how many ``-``'s are replaced by a ``/`` for
# each Autosubmit hierarchy level (to avoid using an if/elif/else).
REPLACE_COUNT = {
    Running.ONCE.value: 1,
    Running.MEMBER.value: 3,
    Running.CHUNK.value: 4
}


def _autosubmit_id_to_ecflow_id(job_id, running):
    """Given an Autosubmit ID, create the node ID for ecFlow (minus heading ``/``)."""
    replace_count = REPLACE_COUNT[running]
    return job_id.replace(DEFAULT_SEPARATOR, '/', replace_count)


def _parse_args(args) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='autosubmit generate ... engine=pyflow',
        description='Produces a valid PyFlow workflow configuration given an Autosubmit experiment ID',
        epilog='This program needs access to an Autosubmit installation'
    )
    parser.add_argument('-e', '--experiment', required=True, help='Autosubmit experiment ID')
    parser.add_argument('-d', '--deploy', default=False, action='store_true', help='Deploy to ecFlow or not')
    parser.add_argument('-o', '--output', default=tempfile.gettempdir(), help='Output directory')
    parser.add_argument('-s', '--server', default='localhost',
                        help='ecFlow server hostname or IP (only used if deploy=True)')
    parser.add_argument('-p', '--port', default=3141, help='ecFlow server port (only used if deploy=True)')
    parser.add_argument('-g', '--graph', default=False, action='store_true', help='Print the DOT plot')
    parser.add_argument('-q', '--quiet', default=False, action='store_true')

    return parser.parse_args(args)


def _create_ecflow_suite(
        experiment_id: str,
        start_dates: List[str],
        members: List[str],
        chunks: [int],
        jobs: List[Job],
        server_host: str,
        output_dir: str) -> Suite:
    """Replicate the vanilla workflow graph structure."""

    # From: https://pyflow-workflow-generator.readthedocs.io/en/latest/content/introductory-course/getting-started.html
    scratch_dir = os.path.join(os.path.abspath(output_dir), 'scratch')
    files_dir = os.path.join(scratch_dir, 'files')
    out_dir = os.path.join(scratch_dir, 'out')

    if not os.path.exists(files_dir):
        os.makedirs(files_dir, exist_ok=True)

    if not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    # First we create a suite with the same ID as the Autosubmit experiment,
    # and families for each Autosubmit hierarchy level.
    # NOTE: PyFlow does not work very well with MyPy: https://github.com/ecmwf/pyflow/issues/5
    with Suite(  # typing: ignore
            experiment_id,
            host=pf.LocalHost(server_host),
            defstatus=pf.state.suspended,  # type: ignore
            home=out_dir,  # type: ignore
            files=files_dir  # type: ignore
    ) as s:  # typing: ignore
        for start_date in start_dates:
            with AnchorFamily(start_date, START_DATE=start_date):  # type: ignore
                for member in members:
                    with AnchorFamily(member, MEMBER=member) as m:  # type: ignore
                        for chunk in chunks:
                            AnchorFamily(str(chunk), CHUNK=chunk)
                            # TODO: splits
        # PyFlow API makes it very easy to create tasks having the ecFlow ID.
        # Due to how we expanded the Autosubmit graph to include the ID's, and how
        # we structured this suite, an Autosubmit ID can be seamlessly translated
        # to an ecFlow ID by simply replacing `_`'s by `/`, ignoring the `_`'s in
        # tasks names.
        #
        # This means that `a000_REMOTE_SETUP` from Autosubmit is `a000/REMOTE_SETUP`
        # in ecFlow, `a000_20220401_fc0_INI` is `a000/20220401/fc0/INI`, and so on.
        for job in jobs:
            ecflow_node = _autosubmit_id_to_ecflow_id(job.long_name, job.running)
            t = Task(job.section)

            # Find the direct parent of the task, based on the Autosubmit task ID.
            # Start from the Suite, and skip the first (suite), and the last (task)
            # as we know we can discard these.
            parent_node = s
            for node in ecflow_node.split('/')[1:-1]:
                parent_node = parent_node[node]
            # We just need to prevent adding a node twice since creating a task automatically adds
            # it to the suite in the context. And simply call ``add_node`` and we should have it.
            if t.name not in list(parent_node.children.mapping.keys()):
                parent_node.add_node(t)

            # Dependencies
            for parent in job.parents:
                dependency_node = _autosubmit_id_to_ecflow_id(parent.long_name, parent.running)
                parent_node = s
                for node in dependency_node.split('/')[1:-1]:
                    parent_node = parent_node[node]
                dependency_node = parent_node[parent.section]

                # Operator overloaded in PyFlow. This creates a dependency.
                dependency_node >> t

            # Script
            # N.B.: The PyFlow documentation states that it is recommended
            # to minimize the number of variables exposed to scripts. We
            # are exposing every parameter available in AS, which is not
            # really recommended. Maybe there is a better way?
            # Note too, that we need to use ``job.file``, not ``job.script_name``,
            # as ``script_name`` is the final generated AS script name, not the
            # job script from the job configuration.
            autosubmit_project_script = os.path.join(files_dir, job.file)
            script = pf.FileScript(autosubmit_project_script)

            for key, value in [(k, v) for k, v in job.parameters.items() if k.isupper()]:
                script.define_environment_variable(key, value)
            for variable in job.undefined_variables:
                script.define_environment_variable(variable, '')
            t.script = script


        return s


def generate(job_list: JobList, options: List[str]) -> None:
    """Generates a PyFlow workflow using Autosubmit database.

    The ``autosubmit create`` command must have been already executed prior
    to calling this function. This is so that the jobs are correctly loaded
    to produce the PyFlow workflow.

    :param job_list: ``JobList`` Autosubmit object, that contains the parameters, jobs, and graph
    :param options: a list of strings with arguments (equivalent to sys.argv), passed to argparse
    """
    args: argparse.Namespace = _parse_args(options)

    expid = job_list.expid
    start_dates = [d.strftime("%Y%m%d") for d in job_list.get_date_list()]
    members = job_list.get_member_list()
    chunks = job_list.get_chunk_list()
    # TODO: splits?

    suite = _create_ecflow_suite(
        experiment_id=expid,
        start_dates=start_dates,
        members=members,
        chunks=chunks,
        jobs=job_list.get_all(),
        server_host=args.server,
        output_dir=args.output
    )

    suite.check_definition()
    if not args.quiet:
        print(suite)

    if args.deploy:
        suite.deploy_suite(overwrite=True)  # type: ignore
        suite.replace_on_server(host=args.server, port=args.port)
