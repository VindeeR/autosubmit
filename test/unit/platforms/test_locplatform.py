# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
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

"""Tests for the Autosubmit Slurm platform."""

import pytest

from textwrap import dedent

from autosubmit.platforms.slurmplatform import SlurmPlatform


@pytest.fixture
def slurm_platform(autosubmit_config) -> SlurmPlatform:
    """Fixture to create a Slurm platform object."""
    as_conf = autosubmit_config('a000', {})
    return SlurmPlatform(as_conf.expid, 'TestSlurmPlatform', as_conf.experiment_data)
