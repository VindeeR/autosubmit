from autosubmit.job.job_common import Status
from autosubmit.job.job import Job
from autosubmit.monitor.monitor import Monitor


def test_check_final_status():
    # Create mock jobs
    parent_job = Job("parent_job", "1", Status.COMPLETED, 0)
    child_job = Job("child_job", "2", Status.WAITING, 0)

    # Set up edge_info for the child job
    child_job.edge_info = {
        "COMPLETED": {
            "parent_job": (parent_job, 0, True)
        }
    }

    # Create an instance of the class containing _check_final_status
    monitor_instance = Monitor()  # Replace with the actual class name if different

    # Call the method
    color, label, dashed = monitor_instance._check_final_status(parent_job, child_job)

    # Assert the expected values
    assert color == monitor_instance._table[Status.KEY_TO_VALUE["COMPLETED"]]
    assert label == 0
    assert dashed is True
