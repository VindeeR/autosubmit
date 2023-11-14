import pytest

from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmit.autosubmit import Autosubmit
from autosubmit.platforms.submitter import Submitter
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from pathlib import Path

from typing import Callable, List, Tuple

from textwrap import dedent

SECTION = 'ARM'
OUT = dedent("""\
        JOB_ID     ST  REASON                         
        167727     EXT COMPLETED               
        167728     RNO -               
        167729     RNE -               
        167730     RUN -               
        167732     ACC -               
        167733     QUE -               
        167734     RNA -               
        167735     RNP -               
        167736     HLD ASHOLD               
        167737     ERR -               
        167738     CCL -               
        167739     RJT -  
        """)


@pytest.mark.parametrize(
    'jobs, cmd, status, reasons',
    [
        (['167727'], '167727', 'COMPLETED', ['COMPLETED']),
        (["167728", "167729", "167730"], '167728+167729+167730', 'RUNNING', ['-', '-', '-']),
        (["167732", "167733", "167734", "167735", "167736"], '167732+167733+167734+167735+167736', 'QUEUING',
         ['-', '-', '-', '-', 'ASHOLD']),
        (["167737", "167738", "167739"], '167737+167738+167739', 'FAILED', ['-', '-', '-']),
        (["3442432423", "238472364782", "1728362138712"], '3442432423+238472364782+1728362138712', None,
         [[], [], []])
    ]
)
def test_parse_all_jobs_output(
        jobs: List[str], cmd: str, status: str, reasons: List[str],
        pjm_setup: Tuple[AutosubmitConfig, Submitter, ParamikoPlatform]
):
    """Test parsing of all jobs output."""
    as_conf, submitter, remote_platform = pjm_setup
    for job_id, reason in zip(jobs, reasons):
        if status is not None:
            assert remote_platform.parse_Alljobs_output(OUT, job_id) in remote_platform.job_status[status]
        else:
            assert remote_platform.parse_Alljobs_output(OUT, job_id) == []
        assert remote_platform.parse_queue_reason(OUT, job_id) == reason


@pytest.fixture
def pjm_setup(
        autosubmit_exp: Callable,
        create_as_conf: Callable) -> Tuple[AutosubmitConfig, Submitter, ParamikoPlatform]:
    exp = autosubmit_exp('a000')
    as_conf = create_as_conf(
        autosubmit_exp=exp,
        yaml_files=[
            Path(__file__).resolve().parent / "files/fake-jobs.yml",
            Path(__file__).resolve().parent / "files/fake-platforms.yml"
        ],
        experiment_data={
            'DEFAULT': {
                'HPCARCH': 'ARM'
            }
        }
    )
    submitter = Autosubmit._get_submitter(as_conf)
    submitter.load_platforms(as_conf)
    remote_platform = submitter.platforms[SECTION]
    return as_conf, submitter, remote_platform


def test_get_submitted_job_id(pjm_setup: Tuple[AutosubmitConfig, Submitter, ParamikoPlatform]):
    """Test parsing of submitted job id."""
    submitted_ok = "[INFO] PJM 0000 pjsub Job 167661 submitted."
    submitted_fail = "[ERR.] PJM 0057 pjsub node=32 is greater than the upper limit (24)."
    as_conf, submitter, remote_platform = pjm_setup
    output = remote_platform.get_submitted_job_id(submitted_ok)
    assert output == [167661]
    output = remote_platform.get_submitted_job_id(submitted_fail)
    assert output == []
