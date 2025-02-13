# Fixtures available to multiple test files must be created in this file.
import tempfile
from contextlib import suppress

import pytest
from dataclasses import dataclass
from pathlib import Path
from ruamel.yaml import YAML
from shutil import rmtree
from tempfile import TemporaryDirectory
from typing import Any, Dict, Callable, List, Protocol, Optional
import os

from autosubmit.autosubmit import Autosubmit
from autosubmit.platforms.slurmplatform import SlurmPlatform, ParamikoPlatform
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory


FAKE_EXP_DIR = "./tests/experiments/"


@dataclass
class AutosubmitExperiment:
    """This holds information about an experiment created by Autosubmit."""
    expid: str
    autosubmit: Autosubmit
    exp_path: Path
    tmp_dir: Path
    aslogs_dir: Path
    status_dir: Path
    platform: ParamikoPlatform


@pytest.fixture(scope="session")
def fixture_temp_dir_copy():
    """
    Fixture that copies the contents of the FAKE_EXP_DIR to a temporary directory with rsync
    """
    with tempfile.TemporaryDirectory() as tempdir:
        # Copy all files recursively
        # os.system(f"rsync -r {FAKE_EXP_DIR} {tempdir}")
        yield tempdir


@pytest.fixture(scope="session")
def fixture_gen_rc_sqlite(fixture_temp_dir_copy: str) -> str:
    """
    Fixture that generates a .autosubmitrc file in the temporary directory
    """
    rc_file = Path(fixture_temp_dir_copy) / ".autosubmitrc"
    with open(rc_file, "w") as f:
        f.write(
            "\n".join(
                [
                    "[database]",
                    f"path = {fixture_temp_dir_copy}",
                    "filename = autosubmit.db",
                    "backend = sqlite",
                    "[local]",
                    f"path = {fixture_temp_dir_copy}",
                    "[globallogs]",
                    f"path = {fixture_temp_dir_copy}/logs",
                    "[historicdb]",
                    f"path = {fixture_temp_dir_copy}/metadata/data",
                    "[structures]",
                    f"path = {fixture_temp_dir_copy}/metadata/structures",
                    "[historiclog]",
                    f"path = {fixture_temp_dir_copy}/metadata/logs",
                    "[graph]",
                    f"path = {fixture_temp_dir_copy}/metadata/graph",
                ]
            )
        )
    yield fixture_temp_dir_copy


@pytest.fixture
def fixture_sqlite(fixture_gen_rc_sqlite: str, monkeypatch: pytest.MonkeyPatch) -> fixture_gen_rc_sqlite:
    monkeypatch.setenv(
        "AUTOSUBMIT_CONFIGURATION", os.path.join(fixture_gen_rc_sqlite, ".autosubmitrc")
    )
    print(f'fixture_gen_rc_sqlite: {fixture_gen_rc_sqlite}')
    yield fixture_gen_rc_sqlite


@pytest.fixture(scope='function')
def autosubmit_exp(autosubmit: Autosubmit, request: pytest.FixtureRequest) -> Callable:
    """Create an instance of ``Autosubmit`` with an experiment."""

    original_root_dir = BasicConfig.LOCAL_ROOT_DIR
    tmp_dir = TemporaryDirectory()
    tmp_path = Path(tmp_dir.name)


    def _create_autosubmit_exp(expid: str):
        root_dir = tmp_path
        BasicConfig.LOCAL_ROOT_DIR = str(root_dir)
        exp_path = BasicConfig.expid_dir(expid)
        
        # directories used when searching for logs to cat
        exp_tmp_dir = BasicConfig.expid_tmp_dir(expid) 
        aslogs_dir = BasicConfig.expid_aslog_dir(expid) 
        status_dir =exp_path / 'status'
        if not os.path.exists(aslogs_dir):
            os.makedirs(aslogs_dir)
        if not os.path.exists(status_dir):
            os.makedirs(status_dir)
        
        platform_config = {
            "LOCAL_ROOT_DIR": BasicConfig.LOCAL_ROOT_DIR,
            "LOCAL_TMP_DIR": str(exp_tmp_dir),
            "LOCAL_ASLOG_DIR": str(aslogs_dir)
        }
        platform = SlurmPlatform(expid=expid, name='slurm_platform', config=platform_config)
        platform.job_status = {
            'COMPLETED': [],
            'RUNNING': [],
            'QUEUING': [],
            'FAILED': []
        }
        submit_platform_script = aslogs_dir.joinpath('submit_local.sh')
        submit_platform_script.touch(exist_ok=True)

        return AutosubmitExperiment(
            expid=expid,
            autosubmit=autosubmit,
            exp_path=exp_path,
            tmp_dir=exp_tmp_dir,
            aslogs_dir=aslogs_dir,
            status_dir=status_dir,
            platform=platform
        )

    def finalizer():
        BasicConfig.LOCAL_ROOT_DIR = original_root_dir
        if tmp_path and tmp_path.exists():
            rmtree(tmp_path)

    request.addfinalizer(finalizer)

    return _create_autosubmit_exp


@pytest.fixture(scope='module')
def autosubmit() -> Autosubmit:
    """Create an instance of ``Autosubmit``.

    Useful when you need ``Autosubmit`` but do not need any experiments."""
    autosubmit = Autosubmit()
    return autosubmit


@pytest.fixture(scope='function')
def create_as_conf() -> Callable:  # May need to be changed to use the autosubmit_config one
    def _create_as_conf(autosubmit_exp: AutosubmitExperiment, yaml_files: List[Path], experiment_data: Dict[str, Any]):
        conf_dir = autosubmit_exp.exp_path.joinpath('conf')
        conf_dir.mkdir(parents=False, exist_ok=False)
        basic_config = BasicConfig
        parser_factory = YAMLParserFactory()
        as_conf = AutosubmitConfig(
            expid=autosubmit_exp.expid,
            basic_config=basic_config,
            parser_factory=parser_factory
        )
        for yaml_file in yaml_files:
            with open(conf_dir / yaml_file.name, 'w+') as f:
                f.write(yaml_file.read_text())
                f.flush()
        # add user-provided experiment data
        with open(conf_dir / 'conftest_as.yml', 'w+') as f:
            yaml = YAML()
            yaml.indent(sequence=4, offset=2)
            yaml.dump(experiment_data, f)
            f.flush()
        return as_conf

    return _create_as_conf

class AutosubmitConfigFactory(Protocol):  # Copied from the autosubmit config parser, that I believe is a revised one from the create_as_conf

    def __call__(self, expid: str, experiment_data: Optional[Dict], *args: Any, **kwargs: Any) -> AutosubmitConfig: ...


@pytest.fixture(scope="function")
def autosubmit_config(
        request: pytest.FixtureRequest,
        mocker: "pytest_mock.MockerFixture") -> AutosubmitConfigFactory:
    """Return a factory for ``AutosubmitConfig`` objects.

    Abstracts the necessary mocking in ``AutosubmitConfig`` and related objects,
    so that if we need to modify these, they can all be done in a single place.

    It is able to create any configuration, based on the ``request`` parameters.

    When the function (see ``scope``) finishes, the object and paths created are
    cleaned (see ``finalizer`` below).
    """

    original_root_dir = BasicConfig.LOCAL_ROOT_DIR
    tmp_dir = TemporaryDirectory()
    tmp_path = Path(tmp_dir.name)

    # Mock this as otherwise BasicConfig.read resets our other mocked values above.
    mocker.patch.object(BasicConfig, "read", autospec=True)

    def _create_autosubmit_config(expid: str, experiment_data: Dict = None, *_, **kwargs) -> AutosubmitConfig:
        """Create an instance of ``AutosubmitConfig``."""
        root_dir = tmp_path
        BasicConfig.LOCAL_ROOT_DIR = str(root_dir)
        exp_path = root_dir / expid
        exp_tmp_dir = exp_path / BasicConfig.LOCAL_TMP_DIR
        aslogs_dir = exp_tmp_dir / BasicConfig.LOCAL_ASLOG_DIR
        conf_dir = exp_path / "conf"
        aslogs_dir.mkdir(parents=True)
        conf_dir.mkdir()

        if not expid:
            raise ValueError("No value provided for expid")
        config = AutosubmitConfig(
            expid=expid,
            basic_config=BasicConfig
        )
        if experiment_data is not None:
            config.experiment_data = experiment_data

        for arg, value in kwargs.items():
            setattr(config, arg, value)

        config.current_loaded_files = [conf_dir / 'dummy-so-it-doesnt-force-reload.yml']
        return config

    def finalizer() -> None:
        BasicConfig.LOCAL_ROOT_DIR = original_root_dir
        with suppress(FileNotFoundError):
            rmtree(tmp_path)

    request.addfinalizer(finalizer)

    return _create_autosubmit_config


@pytest.fixture
def prepare_basic_config(tmpdir):
    basic_conf = BasicConfig()
    BasicConfig.DB_DIR = (tmpdir / "exp_root")
    BasicConfig.DB_FILE = "debug.db"
    BasicConfig.LOCAL_ROOT_DIR = (tmpdir / "exp_root")
    BasicConfig.LOCAL_TMP_DIR = "tmp"
    BasicConfig.LOCAL_ASLOG_DIR = "ASLOGS"
    BasicConfig.LOCAL_PROJ_DIR = "proj"
    BasicConfig.DEFAULT_PLATFORMS_CONF = ""
    BasicConfig.CUSTOM_PLATFORMS_PATH = ""
    BasicConfig.DEFAULT_JOBS_CONF = ""
    BasicConfig.SMTP_SERVER = ""
    BasicConfig.MAIL_FROM = ""
    BasicConfig.ALLOWED_HOSTS = ""
    BasicConfig.DENIED_HOSTS = ""
    BasicConfig.CONFIG_FILE_FOUND = False
    return basic_conf
