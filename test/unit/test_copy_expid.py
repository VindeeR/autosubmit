"""Test to check the functionality of the -y flag. It will copy the hpc too."""

import pytest
from autosubmit.autosubmit import Autosubmit


@pytest.fixture(name="description")
def fake_description():
    """ Fake description needed for Autosubmit create
    """
    return "test descript"


def test_create_expid_default_hpc(description):
    """Create expid with the default hcp value (no -H flag defined)

        autosubmit expid  -d "test descript"
    """
    # create default expid
    expid = Autosubmit.expid(description)
    # capture the platform using the "describe"
    describe = Autosubmit.describe(expid)
    hpc_result = describe[4].lower()

    assert hpc_result == "local"


@pytest.mark.parametrize("fake_hpc, expected_hpc", [
    ("mn5", "mn5"),
    ("", "local"),])
def test_create_expid_flag_hpc(description, fake_hpc, expected_hpc):
    """Create expid using the flag -H. Defining a value for the flag and not defining any value for that flag.

        autosubmit expid -H ithaca -d "experiment is about..."
        autosubmit expid -H "" -d "experiment is about..."

        :param fake_hpc: The value for the -H flag (hpc value)
        :param expected_hpc: The value it is expected for the variable hpc
    """
    # create default expid with know hpc
    expid = Autosubmit.expid(description, fake_hpc)
    # capture the platform using the "describe"
    describe = Autosubmit.describe(expid)
    hpc_result = describe[4].lower()

    assert hpc_result == expected_hpc


@pytest.mark.parametrize("fake_hpc, expected_hpc", [
    ("mn5", "mn5"),
    ("", "local"),
    ])
def test_copy_expid(description, fake_hpc, expected_hpc):
    """Copy an experiment without indicating which is the new HPC platform

        autosubmit expid -y a000 -d "experiment is about..."

        :param fake_hpc: The value for the -H flag (hpc value)
        :param expected_hpc: The value it is expected for the variable hpc
    """
    # create default expid with know hpc
    expid = Autosubmit.expid(description, fake_hpc)
    # copy expid
    copy_expid = Autosubmit.expid(description, "", expid)
    # capture the platform using the "describe"
    describe = Autosubmit.describe(copy_expid)
    hpc_result = describe[4].lower()

    assert hpc_result == expected_hpc


# copy expid with specific hpc should not change the hpc value
# autosubmit expid -y a000 -h local -d "experiment is about..."
def test_copy_expid_no(description):
    """Copy an experiment with specific HPC platform

        autosubmit expid -y a000 -h local -d "experiment is about..."
    """
    # create default expid with know hpc
    fake_hpc = "mn5"
    new_hpc = "local"
    expid = Autosubmit.expid(description, fake_hpc)
    copy_expid = Autosubmit.expid(description, new_hpc, expid)
    # capture the platform using the "describe"
    describe = Autosubmit.describe(copy_expid)
    hpc_result = describe[4].lower()

    assert hpc_result != new_hpc
