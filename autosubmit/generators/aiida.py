from pathlib import Path
from functools import cached_property
import warnings
import re
import yaml

from autosubmitconfigparser.config.configcommon import AutosubmitConfig

from autosubmit.job.job_list import JobList
from autosubmit.generators import AbstractGenerator
from autosubmit.platforms.platform import Platform

"""The AiiDA generator for Autosubmit."""

# Autosubmit Task name separator (not to be confused with task and chunk name separator).
DEFAULT_SEPARATOR = '_'


class Generator(AbstractGenerator):
    """Generates an aiida workflow script that initializes all required AiiDA resources.

    The generated file is structures as the following:
    * imports: All required imports
    * init: Initialization of all python resources that need to be instantiated once for the whole script
    * create_orm_nodes: Creation of all AiiDA's object-relational mapping (ORM) nodes covering the creation of computer and codes
    * workgraph_tasks: Creation of the AiiDA-WorkGraph tasks
    * workgraph_deps: Linking of the dependencies between the AiiDA-WorkGraph tasks
    * workgraph_submission: Submission of the AiiDA-WorkGraph
    """
    # TODO EXECUTABLE, WALLCLOCK, PROCESSORS are not supported ATM
    SUPPORTED_JOB_KEYWORDS = ["WALLCLOCK", "PROCESSORS", "MEMORY", "PLATFORM",  # these we need to transfer to aiida
                              "DEPENDENCIES", "FILE", "RUNNING"]  # these are resolved by autosubmit internally
    # TODO QUEUE, USER, MAX_WALLCLOCK, QUEUE, MAX_PROCESSORS are not supported at the moment
    SUPPORTED_PLATFORM_KEYWORDS = ["TYPE", "HOST", "USER", "SCRATCH_DIR", "MAX_WALLCLOCK", "QUEUE", "MAX_PROCESSORS",  #  these we need to transfer to aiida
                                    "PROJECT"]   # these are resolved by autosubmit internally
    
    def __init__(self, job_list: JobList, as_conf: AutosubmitConfig, output: str):
        # TODO think about dichotomy of output vs output_path
        self._output_path = Path(output).absolute()
        if not self._output_path.exists():
            raise ValueError(f"Given `output` {self._output_path} directory does not exist.")
        self._job_list = job_list
        self._as_conf = as_conf

    @classmethod
    def generate(cls, job_list: JobList, as_conf: AutosubmitConfig, output: str) -> None:
        # I Remove for now args usage, I am not sure if this is needed as server_host and output_dir can be referred from HPC_ARCH. But I see the general need that the engine needs extra information that cannot be inferred from an autosubmit config file
        # TODO args that are needed
        # - output-folder: the folder where to put the scripts
        #
        # TODO Questions:
        # - Are in autosubmit the scripts always on the local machine and then copied over to remote?
        # - How is the memory handled?
        # 
        #args: argparse.Namespace = _parse_args(options)

        # TODO will become an argument
        self = cls(job_list, as_conf, output)
        self._validate()
        workflow_script = self._generate_workflow_script()
        (self._output_path / "submit_aiida_workflow.py").write_text(workflow_script)


    @staticmethod
    def get_engine_name() -> str:
        return "AiiDA"

    @staticmethod
    def add_parse_args(parser) -> None:
        parser.add_argument('-o', '--output', dest="output", default=".", help='Output directory')

    def _validate(self) -> None:
        ## validate jobs
        for job_name, job_conf in self._as_conf.starter_conf['JOBS'].items():
            for key in job_conf.keys():
                if key not in Generator.SUPPORTED_JOB_KEYWORDS:
                    msg = f"Found in job {job_name} configuration file key {key} that is not supported for AiiDA generator."
                    warnings.warn(msg)
        ## validate platforms
        for platform_name, platform_conf in self._as_conf.starter_conf['PLATFORMS'].items():
            # only validate the platforms that exist in jobs
            if platform_name in self._platforms_used_in_job.keys():
                for key in platform_conf.keys():
                    if key not in Generator.SUPPORTED_PLATFORM_KEYWORDS:
                        msg = f"Found in platform {platform_name} configuration file key {key} that is not supported for AiiDA generator."
                        warnings.warn(msg)


    @cached_property
    def _platforms_used_in_job(self) -> dict[str, Platform]:
        """"""
        platforms_used_in_jobs = {}
        for job in self._job_list.get_all():
            platforms_used_in_jobs[job.platform.name] = job.platform
        return platforms_used_in_jobs

    def _generate_workflow_script(self) -> str:
        """Generates a PyFlow workflow using Autosubmit database.

        The ``autosubmit create`` command must have been already executed prior
        to calling this function. This is so that the jobs are correctly loaded
        to produce the PyFlow workflow.

        :param job_list: ``JobList`` Autosubmit object, that contains the parameters, jobs, and graph
        :param as_conf: Autosubmit configuration
        :param options: a list of strings with arguments (equivalent to sys.argv), passed to argparse
        """

        imports = self._generate_imports_section()
        init = self._generate_init_section()
        create_orm_nodes = self._generate_create_orm_nodes_section()
        workgraph_tasks = self._generate_workgraph_tasks_section()
        workgraph_deps = self._generate_workgraph_deps_section()
        workgraph_submission = self._generate_workgraph_submission_section()
        return imports + init + create_orm_nodes + workgraph_tasks + workgraph_deps + workgraph_submission



    def _generate_imports_section(self) -> str:
        return """# IMPORTS
import aiida
from aiida import orm
from aiida_workgraph import WorkGraph
from aiida.orm.utils.builders.computer import ComputerBuilder
from aiida.common.exceptions import NotExistent
from pathlib import Path
import yaml

"""

    def _generate_init_section(self) -> str:
        return """# INIT 
aiida.load_profile()
wg = WorkGraph()
tasks = {}

"""

    def _generate_create_orm_nodes_section(self) -> str:
        # aiida computer

        code_section = "# CREATE_ORM_NODES"
        for platform in self._platforms_used_in_job.values():

            # TODO find configure
            #computer.set_minimum_job_poll_interval(0.0)
            #computer.configure()
            from autosubmit.platforms.locplatform import LocalPlatform
            from autosubmit.platforms.slurmplatform import SlurmPlatform
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
            computer_setup_path = Path(self._output_path / f"{platform.name}/{platform.name}-setup.yml")
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
    computer.configure()"""

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

            code_setup_path =  Path(self._output_path / f"{platform.name}/bash@{platform.name}-setup.yml")
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
    print(f"Created and stored bash@{{computer.label}}")"""
            code_section += create_computer + create_code
        code_section += "\n\n"
        return code_section

    def _generate_workgraph_tasks_section(self):
        code_section = "# WORKGRAPH_TASKS"

        for job in self._job_list.get_all():
            script_name = job.create_script(self._as_conf)
            script_path = Path(job._tmp_path, script_name)
            script_text = open(script_path).read()
            # Let's drop the Autosubmit header and tailed.
            trimmed_script_text = re.findall(
                r'# Autosubmit job(.*)# Autosubmit tailer',
                script_text,
                flags=re.DOTALL | re.MULTILINE)[0][1:-1] 
            trimmed_script_path = self._output_path / script_name
            trimmed_script_path.write_text(trimmed_script_text)
            create_task = f"""
tasks["{job.name}"] = wg.add_task(
    "ShellJob",
    name = "{job.name}",
    command = orm.load_code("bash@{job.platform.name}"),
    arguments = ["{{script}}"],
    nodes = {{"script": orm.SinglefileData("{trimmed_script_path}")}}
)"""
            if job.parameters['MEMORY'] != "":
                create_task += f"""
tasks["{job.name}"].set({{"metadata.options.max_memory_kb": {job.parameters['MEMORY']}}})"""
            code_section += create_task
        code_section += "\n\n"
        return code_section

    def _generate_workgraph_deps_section(self):
        code_section = "# WORKGRAPH_DEPS"
        for edge in self._job_list.graph.edges:
            code_section += f"""
tasks["{edge[1]}"].waiting_on.add(tasks["{edge[0]}"])"""
        code_section += "\n\n"
        return code_section


    def _generate_workgraph_submission_section(self):
        return """# WORKGRAPH_SUBMISSION 
wg.run()""" # TODO change to submit


__all__ = [
    'Generator'

]
