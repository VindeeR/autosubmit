import os
import tempfile
import networkx as nx
from autosubmit.database import db_structure
from autosubmitconfigparser.config.basicconfig import BasicConfig


class TestDbStructure:
    def test_sqlite(self, fixture_sqlite: BasicConfig):
        G = nx.DiGraph([("a", "b"), ("b", "c"), ("a", "d")])
        G.add_node("z")

        with tempfile.TemporaryDirectory() as tmpdirname:
            # Creates a new SQLite db file
            expid = "SQLi"

            # Table not exists
            assert db_structure.get_structure(expid, tmpdirname) == {}

            # Save table
            db_structure.save_structure(G, expid, tmpdirname)
            assert os.path.exists(os.path.join(tmpdirname, f"structure_{expid}.db"))

            # Get correct data
            structure_data = db_structure.get_structure(expid, tmpdirname)
            assert structure_data == {
                "a": ["d", "b"],
                "b": ["c"],
                "c": [],
                "d": [],
                "z": ["z"],
            } or structure_data == {
                "a": ["b", "d"],
                "b": ["c"],
                "c": [],
                "d": [],
                "z": ["z"],
            }

    def test_postgres(self, fixture_postgres: BasicConfig):
        G = nx.DiGraph([("m", "o"), ("o", "p"), ("o", "q")])
        G.add_node("n")
        expid = "PGXX"

        # Table not exists
        assert db_structure.get_structure(expid, "") == {}

        # Save and get correct data
        db_structure.save_structure(G, expid, "")
        structure_data = db_structure.get_structure(expid, "")
        assert structure_data == {
            "m": ["o"],
            "n": ["n"],
            "o": ["q", "p"],
            "p": [],
            "q": [],
        } or structure_data == {
            "m": ["o"],
            "n": ["n"],
            "o": ["p", "q"],
            "p": [],
            "q": [],
        }
