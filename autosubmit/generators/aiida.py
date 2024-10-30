import argparse
import tempfile
from enum import Enum
from pathlib import Path
from typing import List

from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.basicconfig import BasicConfig

from autosubmit.job.job_list import JobList, Job

import aiida

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

def generate(job_list: JobList, as_conf: AutosubmitConfig, options: List[str]) -> None:
    """Generates a PyFlow workflow using Autosubmit database.

    The ``autosubmit create`` command must have been already executed prior
    to calling this function. This is so that the jobs are correctly loaded
    to produce the PyFlow workflow.

    :param job_list: ``JobList`` Autosubmit object, that contains the parameters, jobs, and graph
    :param as_conf: Autosubmit configuration
    :param options: a list of strings with arguments (equivalent to sys.argv), passed to argparse
    """
    # I Remove for now args usage, I am not sure if this is needed as server_host and output_dir can be referred from HPC_ARCH. But I see the general need that the engine needs extra information that cannot be inferred from an autosubmit config file
    # TODO args that are needed
    # - output-folder: the folder where to put the scripts
    #
    # TODO Questions:
    # - Are in autosubmit the scripts always on the local machine and then copied over to remote?
    # - How is the memory handled?
    # 
    #args: argparse.Namespace = _parse_args(options)

    # TODO will becom an argument
    output_path = Path("/home/alexgo/code/autosubmit/generated")


    aiida_workflow_script_init =  \
    """import aiida
from aiida import orm
from aiida_workgraph import WorkGraph
from aiida.orm.utils.builders.computer import ComputerBuilder
from aiida.common.exceptions import NotExistent
from pathlib import Path
import yaml

aiida.load_profile()

wg = WorkGraph()
tasks = {}
    """

    # aiida computer
    unique_platforms = {}
    for job in job_list.get_all():
        unique_platforms[job.platform.name] = job.platform # TODO is name unique?

    aiida_workflow_nodes_init = ""
    for platform in unique_platforms.values():

        # TODO find configure
        #computer.set_minimum_job_poll_interval(0.0)
        #computer.configure()
        from autosubmit.platforms.locplatform import LocalPlatform
        from autosubmit.platforms.slurmplatform import SlurmPlatform
        import yaml
        if isinstance(platform, LocalPlatform):                       
            # This a bit nasty, because we use the Builder 
            # we need to specify these mpirun stuff
            computer_setup = {
                "label": f"{platform.name}",
                "hostname": f"{platform.host}",
                "work_dir": f"{platform.scratch}",
                "description": "",
                "transport": "core.local",
                "scheduler": "core.direct",
                "mpirun_command": "mpirun -np {tot_num_mpiprocs}",
                "mpiprocs_per_machine": 1, # TODO not sure how aiida core.direct handles this
                "default_memory_per_machine": None,
                "append_text": "",
                "prepend_text": "",
                "use_double_quotes": False,
                "shebang": "#!/bin/bash",
            }
        elif isinstance(platform, SlurmPlatform):
            if platform.processors_per_node is None:
                raise ValueError("")
            computer_setup = {
                "label": f"{platform.name}",
                "hostname": f"{platform.host}",
                "work_dir": f"{platform.scratch}",
                "description": "",
                "transport": "core.ssh",
                "scheduler": "core.slurm",
                "mpirun_command": "mpirun -np {tot_num_mpiprocs}",
                "mpiprocs_per_machine": platform.processors_per_node,
                "default_memory_per_machine": 10000, # TODO not sure how autosubmit handles this
                "append_text": "",
                "prepend_text": "",
                "use_double_quotes": False,
                "shebang": "#!/bin/bash",
            }
        else:
            raise ValueError(f"Platform type {platform} not supported for engine aiida.")

        #num_machines`, `num_mpiprocs_per_machine` or `tot_num_mpiprocs
        computer_setup_path = Path(output_path / f"{platform.name}/{platform.name}-setup.yml")
        computer_setup_path.parent.mkdir(exist_ok=True)
        computer_setup_path.write_text(yaml.dump(computer_setup))
        create_computer = f"""
try:
    computer = orm.load_computer("{platform.name}")
except NotExistent:
    setup_path = Path("{computer_setup_path}")
    config_kwargs = yaml.safe_load(setup_path.read_text())
    computer = ComputerBuilder(**config_kwargs).new().store()
    computer.configure(safe_interval=0.0)
    computer.set_minimum_job_poll_interval(0.0)
    computer.configure()
        """

        # aiida bash code to run script
        code_setup = {
            "computer": f"{platform.name}",
            "filepath_executable": "/bin/bash",
            "label": "bash",
            "description": '',
            "default_calc_job_plugin": 'core.shell',
            "prepend_text": '',
            "append_text": '',
            "use_double_quotes": False,
            "with_mpi": False
        }

        #    "computer": f"{platform.name}",
        #    "filepath_executable": "/bin/bash",
        #    "label": "bash",
        #    "description": '',
        #    "default_calc_job_plugin": 'core.shell',
        #    "prepend_text": '',
        #    "append_text": '',
        #    "use_double_quotes": False,
        #    "with_mpi": False

        code_setup_path =  Path(output_path / f"{platform.name}/bash@{platform.name}-setup.yml")
        code_setup_path.parent.mkdir(exist_ok=True)
        code_setup_path.write_text(yaml.dump(code_setup))
        create_code =  f"""
try:
    bash_code = orm.load_code("bash@{platform.name}")
except NotExistent:
    setup_path = Path("{code_setup_path}")
    setup_kwargs = yaml.safe_load(setup_path.read_text())
    setup_kwargs["computer"] = orm.load_computer(setup_kwargs["computer"])
    bash_code = orm.InstalledCode(**setup_kwargs).store()
    print(f"Created and stored bash@{{computer.label}}")
        """
        aiida_workflow_nodes_init += create_computer + create_code

    # aiida tasks
    aiida_workflow_script_tasks = ""
    for job in job_list.get_all():
        script_name = job.create_script(as_conf)
        script_path = Path(job._tmp_path, script_name)
        script_text = open(script_path).read()
        # Let's drop the Autosubmit header and tailed.
        import re
        trimmed_script_text = re.findall(
            r'# Autosubmit job(.*)# Autosubmit tailer',
            script_text,
            flags=re.DOTALL | re.MULTILINE)[0][1:-1] 
        trimmed_script_path = output_path / script_name
        with open(trimmed_script_path, mode="w") as trimmed_script_file:
            trimmed_script_file.write(trimmed_script_text)
        create_task = f"""
tasks["{job.name}"] = wg.add_task(
    "ShellJob",
    name = "{job.name}",
    command = orm.load_code("bash@{job.platform.name}"),
    arguments = ["{{script}}"],
    nodes = {{"script": orm.SinglefileData("{trimmed_script_path}")}}
)
"""
        if job.parameters['MEMORY'] != "":
            create_task += f"""tasks["{job.name}"].set({{"metadata.options.max_memory_kb": {job.parameters['MEMORY']}}})
"""
        aiida_workflow_script_tasks += create_task

    aiida_workflow_script_deps = "\n"
    for edge in job_list.graph.edges:
        add_edges = f"""
tasks["{edge[1]}"].waiting_on.add(tasks["{edge[0]}"])"""
        aiida_workflow_script_deps += add_edges
    aiida_workflow_script_deps = "\n"


    aiida_workflow_script_run = """
wg.run()"""
    aiida_workflow_script_text = aiida_workflow_script_init + aiida_workflow_nodes_init + aiida_workflow_script_tasks + aiida_workflow_script_deps + aiida_workflow_script_run
    (output_path / "submit_aiida_workflow.py").write_text(aiida_workflow_script_text)

__all__ = [
    'generate'
]
