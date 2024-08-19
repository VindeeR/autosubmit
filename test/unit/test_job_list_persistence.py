import pytest
from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job_list_persistence import JobListPersistence, JobListPersistenceDb
from networkx import DiGraph


@pytest.mark.postgres
def test_job_list_persistence_postgres(as_db_postgres):
    job_list_pers = Autosubmit._get_job_list_persistence('job_list_persistence_postgres', None)

    assert isinstance(job_list_pers, JobListPersistence)
    assert isinstance(job_list_pers, JobListPersistenceDb)

    graph = DiGraph(name="test_graph")

    job_list_pers.save('', '', None, graph)

    loaded_graph = job_list_pers.load('', '')

    assert isinstance(loaded_graph, DiGraph)
    assert loaded_graph.name == graph.name

