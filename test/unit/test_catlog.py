# Copyright 2015-2023 Earth Sciences Department, BSC-CNS
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import io
import sys
from contextlib import suppress, redirect_stdout
from pathlib import Path
from pytest import raises
from pytest_mock import MockerFixture
from typing import Callable

from autosubmit.autosubmit import Autosubmit, AutosubmitCritical
from test.unit.conftest import AutosubmitExperiment


def test_invalid_file(autosubmit: Autosubmit):
    with raises(AutosubmitCritical):
        autosubmit.cat_log(None, '8', None)  # type: ignore


def test_invalid_mode(autosubmit: Autosubmit):
    with raises(AutosubmitCritical):
        autosubmit.cat_log(None, 'o', '8')  # type: ignore


# -- workflow

def test_is_workflow_invalid_file(autosubmit: Autosubmit):
    with raises(AutosubmitCritical):
        autosubmit.cat_log('a000', 'j', None)


def test_is_workflow_not_found(mocker, autosubmit: Autosubmit):
    Log = mocker.patch('autosubmit.autosubmit.Log')
    autosubmit.cat_log('a000', 'o', 'c')
    assert Log.info.called
    assert Log.info.call_args[0][0] == 'No logs found.'


def test_is_workflow_log_is_dir(autosubmit_exp: Callable):
    exp = autosubmit_exp('a000')
    log_file_actually_dir = Path(exp.aslogs_dir, 'log_run.log')
    log_file_actually_dir.mkdir()

    with raises(AutosubmitCritical):
        exp.autosubmit.cat_log('a000', 'o', 'c')


def test_is_workflow_out_cat(mocker, autosubmit_exp: Callable):
    exp = autosubmit_exp('a000')
    popen = mocker.patch('subprocess.Popen')
    log_file = Path(exp.aslogs_dir, 'log_run.log')
    with open(log_file, 'w') as f:
        f.write('as test')
        f.flush()
        exp.autosubmit.cat_log('a000', file=None, mode='c')
        assert popen.called
        args = popen.call_args[0][0]
        assert args[0] == 'cat'
        assert args[1] == str(log_file)


def test_is_workflow_status_tail(mocker, autosubmit_exp: Callable):
    popen = mocker.patch('subprocess.Popen')
    exp = autosubmit_exp('a000')
    log_file = Path(exp.status_dir, 'a000_anything.txt')
    with open(log_file, 'w') as f:
        f.write('as test')
        f.flush()
        exp.autosubmit.cat_log('a000', file='s', mode='t')
        assert popen.called
        args = popen.call_args[0][0]
        assert args[0] == 'tail'
        assert str(args[-1]) == str(log_file)


# --- jobs


def test_is_jobs_not_found(mocker, autosubmit_exp: Callable):
    Log = mocker.patch('autosubmit.autosubmit.Log')
    exp = autosubmit_exp('a000')
    for file in ['j', 's', 'o']:
        exp.autosubmit.cat_log('a000_INI', file=file, mode='c')
        assert Log.info.called
        assert Log.info.call_args[0][0] == 'No logs found.'


def test_is_jobs_log_is_dir(autosubmit_exp: Callable, tmp_path: Path):
    exp: AutosubmitExperiment = autosubmit_exp('a000')
    log_file_actually_dir = Path(exp.tmp_dir, 'LOG_a000/a000_INI.20000101.out')
    log_file_actually_dir.mkdir(parents=True)

    with raises(AutosubmitCritical):
        exp.autosubmit.cat_log('a000_INI', 'o', 'c')


def test_is_jobs_out_tail(autosubmit_exp: Callable, tmp_path: Path, mocker: MockerFixture):
    exp: AutosubmitExperiment = autosubmit_exp('a000')
    popen = mocker.patch('subprocess.Popen')
    log_dir = exp.tmp_dir / 'LOG_a000'
    log_dir.mkdir(parents=True)
    log_file = log_dir / 'a000_INI.20200101.out'
    with open(log_file, 'w') as f:
        f.write('as test')
        f.flush()
        exp.autosubmit.cat_log('a000_INI', file=None, mode='t')
        assert popen.called
        args = popen.call_args[0][0]
        assert args[0] == 'tail'
        assert str(args[-1]) == str(log_file)


# --- command-line

def test_command_line_help(mocker):
    args = ['autosubmit', 'cat-log', '--help']
    mocker.patch.context_manager(sys, 'argv', args)
    with io.StringIO() as buf, redirect_stdout(buf):
        with suppress(SystemExit):
            assert Autosubmit.parse_args()
        assert 'View workflow and job logs.' in buf.getvalue()
