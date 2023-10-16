import argparse
import tempfile
from enum import Enum
from pathlib import Path
from typing import List

import pyflow as pf
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.basicconfig import BasicConfig
from pyflow import *

from autosubmit.job.job_list import JobList, Job

"""The PyFlow generator for Autosubmit."""

# Autosubmit Task name separator (not to be confused with task and chunk name separator).
DEFAULT_SEPARATOR = '_'


class Running(Enum):
    """The Running level of an Autosubmit task."""
    ONCE = 'once'
    MEMBER = 'member'
    CHUNK = 'chunk'

    def __str__(self):
        return self.value


# Defines how many ``-``'s are replaced by a ``/`` for
# each Autosubmit hierarchy level (to avoid using an if/elif/else).
REPLACE_COUNT = {
    Running.ONCE.value: 1,
    Running.MEMBER.value: 3,
    Running.CHUNK.value: 4
}


def _autosubmit_id_to_ecflow_id(job_id: str, running: str):
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
        output_dir: str,
        as_conf: AutosubmitConfig) -> Suite:
    """Replicate the vanilla workflow graph structure."""

    # From: https://pyflow-workflow-generator.readthedocs.io/en/latest/content/introductory-course/getting-started.html
    # /scratch is a base directory for ECF_FILES and ECF_HOME
    scratch_dir = Path(Path(output_dir).absolute(), 'scratch')
    # /scratch/files is the ECF_FILES, where ecflow_server looks for ecf scripts if they are not in their default location
    files_dir = scratch_dir / 'files'
    # /scratch/out is the ECF_HOME, the home of all ecFlow files, $CWD
    out_dir = scratch_dir / 'out'

    if not files_dir.exists():
        files_dir.mkdir(parents=True, exist_ok=True)

    if not out_dir.exists():
        out_dir.mkdir(parents=True, exist_ok=True)

    # First we create a suite with the same ID as the Autosubmit experiment,
    # and families for each Autosubmit hierarchy level. We use control variables
    # such as home (ECF_HOME), and files (ECF_FILES), but there are others that
    # can be used too, like include (ECF_INCLUDE), out (ECF_OUT), and extn
    # (ECF_EXTN, defaults to the extension .ecf).
    # NOTE: PyFlow does not work very well with MyPy: https://github.com/ecmwf/pyflow/issues/5
    with Suite(  # typing: ignore
            experiment_id,
            host=pf.LocalHost(server_host),
            defstatus=pf.state.suspended,  # type: ignore
            home=str(out_dir),  # type: ignore
            files=str(files_dir)  # type: ignore
    ) as s:  # typing: ignore
        for start_date in start_dates:
            with AnchorFamily(start_date, START_DATE=start_date):  # type: ignore
                for member in members:
                    with AnchorFamily(member, MEMBER=member):  # type: ignore
                        for chunk in chunks:
                            AnchorFamily(str(chunk), CHUNK=chunk)
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
            if job.split is not None:
                t = Task(f'{job.section}_{job.split}', SPLIT=job.split)
            else:
                t = Task(job.section)

            # Find the direct parent of the task, based on the Autosubmit task ID.
            # Start from the Suite, and skip the first (suite), and the last (task)
            # as we know we can discard these.
            parent_node = s
            for node in ecflow_node.split('/')[1:-1]:
                parent_node = parent_node[node]
            # We just need to prevent adding a node twice since creating a task automatically adds
            # it to the suite in the context. And simply call ``add_node`` and we should have it.
            # TODO: use mapping.keys when Py>=3.8 or 3.9? if t.name not in list(parent_node.children.mapping.keys()):
            if t.name not in [child.name for child in parent_node.children]:
                parent_node.add_node(t)

            # Dependencies
            for parent in job.parents:
                dependency_node = _autosubmit_id_to_ecflow_id(parent.long_name, parent.running)
                parent_node = s
                for node in dependency_node.split('/')[1:-1]:
                    parent_node = parent_node[node]
                dependency_node = parent_node[parent.section]

                # In case we ever need to use the pre-processed file.
                #
                # script_name = job.create_script(as_conf)
                # script_text = open(Path(job._tmp_path, script_name)).read()
                # # Let's drop the Autosubmit header and tailed.
                # script_text = re.findall(
                #     r'# Autosubmit job(.*)# Autosubmit tailer',
                #     script_text,
                #     flags=re.DOTALL | re.MULTILINE)[0][1:-1]
                # t.script = script_text

                # Operator overloaded in PyFlow. This creates a dependency.
                dependency_node >> t

            # Script
            # N.B.: We used the code below in the beginning, but later we realized it would
            #       not work. In Autosubmit, the FILE: $FILE is a file, relative to the AS
            #       Project folder. The $FILE template script is then pre-processed to be
            #       executed by AS. That is different from ecFlow, where the Task Script is
            #       value is pre-processed (i.e. if the value is ``templates/local_script.sh``
            #       that value is treated as a string when included in the final job file)
            #       to generate the ecFlow job file. So ecFlow pre-processes the ``.script``
            #       value, whereas Autosubmit loads the ``FILE`` script and pre-processes it.
            #
            #       In order to have a similar behavior, we insert the contents of the AS
            #       template script as the ecFlow task. That way, the template script (now
            #       a Task Script in ecFlow) will be pre-processed by ecFlow.
            #
            #       The variables may still need to be manually adjusted, but once that is
            #       done, the script should then be ready to be executed (i.e. ported).
            # t.script = job.file
            with open(Path(as_conf.get_project_dir(), job.file)) as f:
                t.script = f.read()

        return s


def generate(job_list: JobList, as_conf: AutosubmitConfig, options: List[str]) -> None:
    """Generates a PyFlow workflow using Autosubmit database.

    The ``autosubmit create`` command must have been already executed prior
    to calling this function. This is so that the jobs are correctly loaded
    to produce the PyFlow workflow.

    :param job_list: ``JobList`` Autosubmit object, that contains the parameters, jobs, and graph
    :param as_conf: Autosubmit configuration
    :param options: a list of strings with arguments (equivalent to sys.argv), passed to argparse
    """
    args: argparse.Namespace = _parse_args(options)

    expid = job_list.expid
    start_dates = [d.strftime("%Y%m%d") for d in job_list.get_date_list()]
    members = job_list.get_member_list()
    chunks = job_list.get_chunk_list()

    suite = _create_ecflow_suite(
        experiment_id=expid,
        start_dates=start_dates,
        members=members,
        chunks=chunks,
        jobs=job_list.get_all(),
        server_host=args.server,
        output_dir=args.output,
        as_conf=as_conf
    )

    suite.check_definition()
    if not args.quiet:
        print(suite)

    if args.deploy:
        suite.deploy_suite(overwrite=True)  # type: ignore
        suite.replace_on_server(host=args.server, port=args.port)


__all__ = [
    'generate'
]
